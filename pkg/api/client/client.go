// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiclient

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/encoding/packfile"
	"github.com/wrgl/wrgl/pkg/objects"
	"golang.org/x/net/publicsuffix"
)

const (
	CTJSON     = "application/json"
	CTPackfile = "application/x-wrgl-packfile"
)

type ClientOption func(c *Client)

func WithHeader(header http.Header) ClientOption {
	return func(c *Client) {
		c.requestOptions = append(c.requestOptions, WithRequestHeader(header))
	}
}

func WithCookies(cookies []*http.Cookie) ClientOption {
	return func(c *Client) {
		c.requestOptions = append(c.requestOptions, WithRequestCookies(cookies))
	}
}

func WithAuthorization(token string) ClientOption {
	return func(c *Client) {
		c.requestOptions = append(c.requestOptions, WithRequestAuthorization(token))
	}
}

func WithTransport(transport http.RoundTripper) ClientOption {
	return func(c *Client) {
		c.client.Transport = transport
	}
}

type RequestOption func(r *http.Request)

func WithRequestHeader(header http.Header) RequestOption {
	return func(r *http.Request) {
		for k, sl := range header {
			for _, v := range sl {
				r.Header.Add(k, v)
			}
		}
	}
}

func WithRequestCookies(cookies []*http.Cookie) RequestOption {
	return func(r *http.Request) {
		for _, c := range cookies {
			r.AddCookie(c)
		}
	}
}

func WithRequestAuthorization(token string) RequestOption {
	return func(r *http.Request) {
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
	}
}

type Client struct {
	client *http.Client
	// origin is the scheme + host name of remote server
	origin         string
	requestOptions []RequestOption
	logger         logr.Logger
}

func NewClient(origin string, logger logr.Logger, opts ...ClientOption) (*Client, error) {
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return nil, err
	}
	c := &Client{
		client: &http.Client{
			Jar: jar,
		},
		origin: origin,
		logger: logger.WithName("Client"),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func (s *Client) LogRequest(r *http.Request, payload interface{}) {
	b, err := json.MarshalIndent(payload, "  ", "  ")
	if err != nil {
		panic(err)
	}
	s.logger.Info("request", "method", r.Method, "url", r.URL, "body", string(b))
}

func (s *Client) LogResponse(r *http.Response, payload interface{}) {
	b, err := json.MarshalIndent(payload, "  ", "  ")
	if err != nil {
		panic(err)
	}
	s.logger.Info("response", "method", r.Request.Method, "url", r.Request.URL, "body", string(b))
}

func parseJSONPayload(resp *http.Response, obj interface{}) (err error) {
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, CTJSON) {
		return fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	return json.Unmarshal(b, obj)
}

func (c *Client) Request(method, path string, body io.Reader, headers map[string]string, opts ...RequestOption) (resp *http.Response, err error) {
	req, err := http.NewRequest(method, c.origin+path, body)
	if err != nil {
		return
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	for _, opt := range c.requestOptions {
		opt(req)
	}
	for _, opt := range opts {
		opt(req)
	}
	resp, err = c.client.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode >= 400 {
		return nil, NewHTTPError(resp)
	}
	return
}

type formFile struct {
	FileName string
	Content  io.Reader
}

func (c *Client) PostMultipartForm(path string, value map[string][]string, files map[string]formFile, opts ...RequestOption) (*http.Response, error) {
	buf := bytes.NewBuffer(nil)
	w := multipart.NewWriter(buf)
	for k, sl := range value {
		for _, v := range sl {
			err := w.WriteField(k, v)
			if err != nil {
				return nil, err
			}
		}
	}
	for k, r := range files {
		if r.Content == nil {
			continue
		}
		w, err := w.CreateFormFile(k, r.FileName)
		if err != nil {
			return nil, err
		}
		io.Copy(w, r.Content)
	}
	err := w.Close()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, c.origin+path, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	for _, opt := range c.requestOptions {
		opt(req)
	}
	for _, opt := range opts {
		opt(req)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, NewHTTPError(resp)
	}
	return resp, nil
}

func (c *Client) GetHead(branch string, opts ...RequestOption) (com *payload.Commit, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/refs/heads/%s/", branch), nil, nil, opts...)
	if err != nil {
		return
	}
	com = &payload.Commit{}
	if err = parseJSONPayload(resp, com); err != nil {
		return nil, err
	}
	return
}

func (c *Client) GetCommitProfile(sum []byte, opts ...RequestOption) (tblProf *objects.TableProfile, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/commits/%x/profile/", sum), nil, nil, opts...)
	if err != nil {
		return
	}
	tblProf = &objects.TableProfile{}
	if err = parseJSONPayload(resp, tblProf); err != nil {
		return nil, err
	}
	return
}

func (c *Client) GetTableProfile(sum []byte, opts ...RequestOption) (tblProf *objects.TableProfile, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/tables/%x/profile/", sum), nil, nil, opts...)
	if err != nil {
		return
	}
	tblProf = &objects.TableProfile{}
	if err = parseJSONPayload(resp, tblProf); err != nil {
		return nil, err
	}
	return
}

func (c *Client) GetCommits(head string, maxDepth int, opts ...RequestOption) (gcr *payload.GetCommitsResponse, err error) {
	query := url.Values{}
	query.Set("head", head)
	query.Set("maxDepth", strconv.Itoa(maxDepth))
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/commits/?%s", query.Encode()), nil, nil, opts...)
	if err != nil {
		return
	}
	gcr = &payload.GetCommitsResponse{}
	if err = parseJSONPayload(resp, gcr); err != nil {
		return nil, err
	}
	return
}

func (c *Client) GetRefs(prefixes, notPrefixes []string, opts ...RequestOption) (m map[string][]byte, err error) {
	path := "/refs/"
	v := url.Values{}
	for _, s := range prefixes {
		v.Add("prefix", s)
	}
	for _, s := range notPrefixes {
		v.Add("notprefix", s)
	}
	if len(v) > 0 {
		path = fmt.Sprintf("%s?%s", path, v.Encode())
	}
	resp, err := c.Request(http.MethodGet, path, nil, nil, opts...)
	if err != nil {
		return
	}
	rr := &payload.GetRefsResponse{}
	if err = parseJSONPayload(resp, rr); err != nil {
		return nil, err
	}
	c.LogResponse(resp, rr)
	m = map[string][]byte{}
	for k, v := range rr.Refs {
		m[k] = (*v)[:]
	}
	return
}

func (c *Client) Commit(branch, message, fileName string, file io.Reader, primaryKey []string, tid *uuid.UUID, opts ...RequestOption) (cr *payload.CommitResponse, err error) {
	value := map[string][]string{
		"branch":     {branch},
		"message":    {message},
		"primaryKey": {strings.Join(primaryKey, ",")},
	}
	if tid != nil {
		value["txid"] = []string{tid.String()}
	}
	resp, err := c.PostMultipartForm(api.PathCommit, value, map[string]formFile{
		"file": {
			FileName: fileName,
			Content:  file,
		},
	}, opts...)
	if err != nil {
		return nil, err
	}
	cr = &payload.CommitResponse{}
	if err = parseJSONPayload(resp, cr); err != nil {
		return nil, err
	}
	return cr, nil
}

func (c *Client) Diff(sum1, sum2 []byte, opts ...RequestOption) (dr *payload.DiffResponse, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/diff/%x/%x/", sum1, sum2), nil, nil, opts...)
	if err != nil {
		return
	}
	dr = &payload.DiffResponse{}
	if err = parseJSONPayload(resp, dr); err != nil {
		return nil, err
	}
	return dr, nil
}

func (c *Client) GetBlocks(commit string, start, end int, format payload.BlockFormat, columns bool, opts ...RequestOption) (resp *http.Response, err error) {
	v := url.Values{}
	v.Set("head", commit)
	if start > 0 {
		v.Set("start", strconv.Itoa(start))
	}
	if end > 0 {
		v.Set("end", strconv.Itoa(end))
	}
	if format != "" {
		v.Set("format", string(format))
	}
	if columns {
		v.Set("columns", "true")
	}
	var qs string
	if len(v) > 0 {
		qs = fmt.Sprintf("?%s", v.Encode())
	}
	return c.Request(http.MethodGet, fmt.Sprintf("/blocks/%s", qs), nil, nil, opts...)
}

func (c *Client) GetTableBlocks(sum []byte, start, end int, format payload.BlockFormat, columns bool, opts ...RequestOption) (resp *http.Response, err error) {
	v := url.Values{}
	if start > 0 {
		v.Set("start", strconv.Itoa(start))
	}
	if end > 0 {
		v.Set("end", strconv.Itoa(end))
	}
	if format != "" {
		v.Set("format", string(format))
	}
	if columns {
		v.Set("columns", "true")
	}
	var qs string
	if len(v) > 0 {
		qs = fmt.Sprintf("?%s", v.Encode())
	}
	return c.Request(http.MethodGet, fmt.Sprintf("/tables/%x/blocks/%s", sum, qs), nil, nil, opts...)
}

func (c *Client) GetRows(commit string, offsets []int, opts ...RequestOption) (resp *http.Response, err error) {
	v := url.Values{}
	v.Set("head", commit)
	if len(offsets) > 0 {
		sl := make([]string, len(offsets))
		for i := range sl {
			sl[i] = strconv.Itoa(offsets[i])
		}
		v.Set("offsets", strings.Join(sl, ","))
	}
	var qs string
	if len(v) > 0 {
		qs = fmt.Sprintf("?%s", v.Encode())
	}
	return c.Request(http.MethodGet, fmt.Sprintf("/rows/%s", qs), nil, nil, opts...)
}

func (c *Client) GetTableRows(sum []byte, offsets []int, opts ...RequestOption) (resp *http.Response, err error) {
	v := url.Values{}
	if len(offsets) > 0 {
		sl := make([]string, len(offsets))
		for i := range sl {
			sl[i] = strconv.Itoa(offsets[i])
		}
		v.Set("offsets", strings.Join(sl, ","))
	}
	var qs string
	if len(v) > 0 {
		qs = fmt.Sprintf("?%s", v.Encode())
	}
	return c.Request(http.MethodGet, fmt.Sprintf("/tables/%x/rows/%s", sum, qs), nil, nil, opts...)
}

func (c *Client) GetCommit(sum []byte, opts ...RequestOption) (cr *payload.Commit, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/commits/%x/", sum), nil, nil, opts...)
	if err != nil {
		return
	}
	cr = &payload.Commit{}
	if err = parseJSONPayload(resp, cr); err != nil {
		return nil, err
	}
	return cr, nil
}

func (c *Client) GetTable(sum []byte, opts ...RequestOption) (tr *payload.GetTableResponse, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/tables/%x/", sum), nil, nil, opts...)
	if err != nil {
		return
	}
	tr = &payload.GetTableResponse{}
	if err = parseJSONPayload(resp, tr); err != nil {
		return nil, err
	}
	return tr, nil
}

func parseUploadPackResult(r io.ReadCloser) (upr *payload.UploadPackResponse, err error) {
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		return
	}
	upr = &payload.UploadPackResponse{}
	err = json.Unmarshal(b, upr)
	if err != nil {
		return nil, err
	}
	return upr, nil
}

func (c *Client) GetObjects(tables [][]byte, opts ...RequestOption) (pr *packfile.PackfileReader, err error) {
	hexes := make([]string, len(tables))
	for i, sum := range tables {
		hexes[i] = hex.EncodeToString(sum)
	}
	q := url.Values{}
	q.Set("tables", strings.Join(hexes, ","))
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/objects/?%s", q.Encode()), nil, nil, opts...)
	if err != nil {
		return
	}
	if ct := resp.Header.Get("Content-Type"); ct != CTPackfile {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	return packfile.NewPackfileReader(resp.Body)
}

func (c *Client) CreateTransaction(req *payload.CreateTransactionRequest, opts ...RequestOption) (ctr *payload.CreateTransactionResponse, err error) {
	var body io.Reader
	var header map[string]string
	if req != nil {
		b, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
		header = map[string]string{
			"Content-Type": CTJSON,
		}
	}
	resp, err := c.Request(http.MethodPost, "/transactions/", body, header, opts...)
	if err != nil {
		return
	}
	ctr = &payload.CreateTransactionResponse{}
	if err = parseJSONPayload(resp, ctr); err != nil {
		return nil, err
	}
	return
}

func (c *Client) GetTransaction(id uuid.UUID, opts ...RequestOption) (gtr *payload.GetTransactionResponse, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/transactions/%s/", id), nil, nil, opts...)
	if err != nil {
		return
	}
	gtr = &payload.GetTransactionResponse{}
	if err = parseJSONPayload(resp, gtr); err != nil {
		return nil, err
	}
	return
}

func (c *Client) updateTransaction(id uuid.UUID, req *payload.UpdateTransactionRequest, opts ...RequestOption) (resp *http.Response, err error) {
	b, err := json.Marshal(req)
	if err != nil {
		return
	}
	return c.Request(http.MethodPost, fmt.Sprintf("/transactions/%s/", id.String()), bytes.NewReader(b), map[string]string{
		"Content-Type": api.CTJSON,
	}, opts...)
}

func (c *Client) DiscardTransaction(id uuid.UUID, opts ...RequestOption) (resp *http.Response, err error) {
	return c.updateTransaction(id, &payload.UpdateTransactionRequest{
		Discard: true,
	}, opts...)
}

func (c *Client) CommitTransaction(id uuid.UUID, opts ...RequestOption) (resp *http.Response, err error) {
	return c.updateTransaction(id, &payload.UpdateTransactionRequest{
		Commit: true,
	}, opts...)
}

func (c *Client) GarbageCollect(opts ...RequestOption) (resp *http.Response, err error) {
	return c.Request(http.MethodPost, "/gc/", nil, nil, opts...)
}

func (c *Client) PostUploadPack(req *payload.UploadPackRequest, opts ...RequestOption) (upr *payload.UploadPackResponse, pr *packfile.PackfileReader, logPayload func(), err error) {
	b, err := json.Marshal(req)
	if err != nil {
		return
	}
	resp, err := c.Request(http.MethodPost, "/upload-pack/", bytes.NewReader(b), map[string]string{
		"Content-Type": CTJSON,
	}, opts...)
	if err != nil {
		return
	}
	c.LogRequest(resp.Request, req)
	switch resp.Header.Get("Content-Type") {
	case CTPackfile:
		pr, err = packfile.NewPackfileReader(resp.Body)
		if err != nil {
			resp.Body.Close()
			return
		}
		logPayload = func() {
			c.LogResponse(resp, pr.Info)
		}
	case CTJSON:
		upr, err = parseUploadPackResult(resp.Body)
		if err != nil {
			return
		}
		logPayload = func() {
			c.LogResponse(resp, upr)
		}
	default:
		resp.Body.Close()
		err = fmt.Errorf("unrecognized content-type %q", resp.Header.Get("Content-Type"))
	}
	return
}

func (c *Client) PostReceivePack(updates map[string]*payload.Update, tableHaves [][]byte, opts ...RequestOption) (*http.Response, error) {
	req := &payload.ReceivePackRequest{
		Updates:    updates,
		TableHaves: payload.BytesSliceToHexSlice(tableHaves),
	}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.Request(http.MethodPost, "/receive-pack/", bytes.NewReader(b), map[string]string{
		"Content-Type": CTJSON,
	}, opts...)
	if err != nil {
		return nil, err
	}
	c.LogRequest(resp.Request, req)
	return resp, nil
}

func (c *Client) ErrHTTP(resp *http.Response) error {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
}
