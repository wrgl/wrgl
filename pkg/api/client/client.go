// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiclient

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

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
	invalidRPT = "invalid rpt"
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

func WithTransport(transport http.RoundTripper) ClientOption {
	return func(c *Client) {
		c.client.Transport = transport
	}
}

func WithUMATicketHandler(cb func(asURI, ticket, oldRPT string, logger logr.Logger) (rpt string, err error)) ClientOption {
	return func(c *Client) {
		c.umaTicketHandler = cb
	}
}

func WithRelyingPartyToken(rpt string) ClientOption {
	return func(c *Client) {
		c.rpt = rpt
	}
}

func WithForceAuthenticate() ClientOption {
	return func(c *Client) {
		c.rpt = invalidRPT
	}
}

type Client struct {
	client *http.Client
	// origin is the scheme + host name of remote server
	origin           string
	requestOptions   []RequestOption
	logger           logr.Logger
	rpt              string
	umaTicketHandler func(asURI, ticket, oldRPT string, logger logr.Logger) (rpt string, err error)
	bufPool          *sync.Pool
}

func NewClient(origin string, logger logr.Logger, opts ...ClientOption) (*Client, error) {
	c := &Client{
		client: &http.Client{},
		origin: origin,
		bufPool: &sync.Pool{
			New: func() any {
				return NewReplayableBuffer()
			},
		},
		logger: logger.WithName("Client"),
	}
	if err := c.ResetCookies(); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func (c *Client) ResetCookies() (err error) {
	c.client.Jar, err = cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	return
}

var authHeaderRegex = regexp.MustCompile(`UMA\s+realm="([^"]+)",\s+as_uri="([^"]+)",\s+ticket="([^"]+)"`)

func ExtractTicketFrom401(resp *http.Response) (asUri, ticket string) {
	if http.StatusUnauthorized == resp.StatusCode {
		matches := authHeaderRegex.FindStringSubmatch(resp.Header.Get("WWW-Authenticate"))
		if matches != nil {
			return matches[2], matches[3]
		}
	}
	return "", ""
}

type ErrUnauthorized struct {
	AuthServerURI string
	Ticket        string
}

func (err *ErrUnauthorized) Error() string {
	return fmt.Sprintf("unauthorized, login at %s with ticket %q", err.AuthServerURI, err.Ticket)
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

func (c *Client) doRequest(req *http.Request, opts ...RequestOption) (resp *http.Response, err error) {
	for _, opt := range c.requestOptions {
		opt(req)
	}
	for _, opt := range opts {
		opt(req)
	}
	logger := c.logger.WithValues(
		"method", req.Method,
		"path", req.URL.Path,
	)
	if c.rpt != "" {
		logger.Info("using existing rpt")
		req.Header.Set("Authorization", "Bearer "+c.rpt)
	} else {
		logger.Info("no existing rpt")
	}
	start := time.Now()
	resp, err = c.client.Do(req)
	if err != nil {
		return
	}
	logger.Info("response",
		"code", resp.StatusCode,
		"elapsed", time.Since(start),
	)
	if asURI, ticket := ExtractTicketFrom401(resp); ticket != "" {
		if c.umaTicketHandler == nil {
			return nil, &ErrUnauthorized{asURI, ticket}
		}
		if c.rpt == invalidRPT {
			c.rpt = ""
		}
		var rpt string
		start := time.Now()
		rpt, err = c.umaTicketHandler(asURI, ticket, c.rpt, logger)
		if err != nil {
			return nil, err
		}
		logger.Info("fetched new rpt",
			"elapsed", time.Since(start),
		)
		c.rpt = rpt
		oldHeader := req.Header
		body := req.Body.(*ReplayableBuffer)
		if body != nil {
			body.Seek(0, io.SeekStart)
		}
		req, err = http.NewRequest(req.Method, req.URL.String(), body)
		if err != nil {
			return nil, err
		}
		req.Header = oldHeader
		req.Header.Set("Authorization", "Bearer "+c.rpt)
		start = time.Now()
		resp, err = c.client.Do(req)
		if err != nil {
			return nil, err
		}
		logger.Info("retry response",
			"code", resp.StatusCode,
			"elapsed", time.Since(start),
		)
	}
	return resp, nil
}

func (c *Client) Request(method, path string, body *ReplayableBuffer, headers map[string]string, opts ...RequestOption) (resp *http.Response, err error) {
	if body != nil {
		body.Seek(0, io.SeekStart)
	}
	req, err := http.NewRequest(method, c.origin+path, body)
	if err != nil {
		return
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err = c.doRequest(req, opts...)
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
	body := c.bufPool.Get().(*ReplayableBuffer)
	defer c.bufPool.Put(body)
	body.Reset()
	w := multipart.NewWriter(body)
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
	body.Seek(0, io.SeekStart)
	req, err := http.NewRequest(http.MethodPost, c.origin+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := c.doRequest(req, opts...)
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
	var body *ReplayableBuffer
	var header map[string]string
	if req != nil {
		b, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		body = c.bufPool.Get().(*ReplayableBuffer)
		defer c.bufPool.Put(body)
		body.Reset()
		_, err = body.Write(b)
		if err != nil {
			return nil, err
		}
		body.Seek(0, io.SeekStart)
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

func (c *Client) JsonRequest(method, path string, req any, opts ...RequestOption) (*http.Response, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	body := c.bufPool.Get().(*ReplayableBuffer)
	defer c.bufPool.Put(body)
	body.Reset()
	_, err = body.Write(b)
	if err != nil {
		return nil, err
	}
	body.Seek(0, io.SeekStart)
	return c.Request(method, path, body, map[string]string{
		"Content-Type": api.CTJSON,
	}, opts...)
}

func (c *Client) updateTransaction(id uuid.UUID, req *payload.UpdateTransactionRequest, opts ...RequestOption) (resp *http.Response, err error) {
	return c.JsonRequest(http.MethodPost, fmt.Sprintf("/transactions/%s/", id.String()), req, opts...)
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

func (c *Client) PostUploadPack(req *payload.UploadPackRequest, opts ...RequestOption) (upr *payload.UploadPackResponse, pr *packfile.PackfileReader, err error) {
	resp, err := c.JsonRequest(http.MethodPost, "/upload-pack/", req, opts...)
	if err != nil {
		return
	}
	switch resp.Header.Get("Content-Type") {
	case CTPackfile:
		pr, err = packfile.NewPackfileReader(resp.Body)
		if err != nil {
			resp.Body.Close()
			return
		}
	case CTJSON:
		upr, err = parseUploadPackResult(resp.Body)
		if err != nil {
			return
		}
	default:
		resp.Body.Close()
		err = fmt.Errorf("unrecognized content-type %q", resp.Header.Get("Content-Type"))
	}
	return
}

func (c *Client) PostReceivePack(updates map[string]*payload.Update, tableHaves [][]byte, opts ...RequestOption) (*http.Response, error) {
	resp, err := c.JsonRequest(http.MethodPost, "/receive-pack/", &payload.ReceivePackRequest{
		Updates:    updates,
		TableHaves: payload.BytesSliceToHexSlice(tableHaves),
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ErrHTTP(resp *http.Response) error {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
}
