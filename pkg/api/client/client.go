// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"

	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/encoding"
	"golang.org/x/net/publicsuffix"
)

const (
	CTJSON     = "application/json"
	CTPackfile = "application/x-wrgl-packfile"
)

type RequestOption func(r *http.Request)

func WithHeader(header http.Header) RequestOption {
	return func(r *http.Request) {
		for k, sl := range header {
			for _, v := range sl {
				r.Header.Add(k, v)
			}
		}
	}
}

func WithAuthorization(token string) RequestOption {
	return func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+token)
	}
}

type Client struct {
	client *http.Client
	// origin is the scheme + host name of remote server
	origin         string
	requestOptions []RequestOption
}

func NewClient(origin string, opts ...RequestOption) (*Client, error) {
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return nil, err
	}
	return &Client{
		client: &http.Client{
			Jar: jar,
		},
		origin:         origin,
		requestOptions: opts,
	}, nil
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

func (c *Client) Authenticate(email, password string, opts ...RequestOption) (token string, err error) {
	b, err := json.Marshal(&payload.AuthenticateRequest{
		Email:    email,
		Password: password,
	})
	if err != nil {
		return
	}
	resp, err := c.Request(http.MethodPost, "/authenticate/", bytes.NewReader(b), map[string]string{
		"Content-Type": CTJSON,
	}, opts...)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return "", fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	ar := &payload.AuthenticateResponse{}
	err = json.Unmarshal(b, ar)
	if err != nil {
		return
	}
	return ar.IDToken, nil
}

func (c *Client) GetConfig(opts ...RequestOption) (cfg *conf.Config, err error) {
	resp, err := c.Request(http.MethodGet, "/config/", nil, nil, opts...)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	cfg = &conf.Config{}
	err = json.Unmarshal(b, cfg)
	if err != nil {
		return nil, err
	}
	return
}

func (c *Client) PutConfig(cfg *conf.Config, opts ...RequestOption) (resp *http.Response, err error) {
	b, err := json.Marshal(cfg)
	if err != nil {
		return
	}
	return c.Request(http.MethodPut, "/config/", bytes.NewReader(b), map[string]string{
		"Content-Type": CTJSON,
	}, opts...)
}

func (c *Client) GetHead(branch string, opts ...RequestOption) (com *payload.Commit, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/refs/heads/%s/", branch), nil, nil, opts...)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	com = &payload.Commit{}
	err = json.Unmarshal(b, com)
	if err != nil {
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
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	gcr = &payload.GetCommitsResponse{}
	err = json.Unmarshal(b, gcr)
	if err != nil {
		return nil, err
	}
	return
}

func (c *Client) GetRefs(opts ...RequestOption) (m map[string][]byte, err error) {
	resp, err := c.Request(http.MethodGet, "/refs/", nil, nil, opts...)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	rr := &payload.GetRefsResponse{}
	err = json.Unmarshal(b, rr)
	if err != nil {
		return
	}
	m = map[string][]byte{}
	for k, v := range rr.Refs {
		m[k] = (*v)[:]
	}
	return
}

func (c *Client) Commit(branch, message, fileName string, file io.Reader, primaryKey []string, opts ...RequestOption) (cr *payload.CommitResponse, err error) {
	resp, err := c.PostMultipartForm(api.PathCommit, map[string][]string{
		"branch":     {branch},
		"message":    {message},
		"primaryKey": primaryKey,
	}, map[string]formFile{
		"file": {
			FileName: fileName,
			Content:  file,
		},
	}, opts...)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	cr = &payload.CommitResponse{}
	err = json.Unmarshal(b, cr)
	if err != nil {
		return nil, err
	}
	return cr, nil
}

func (c *Client) Diff(sum1, sum2 []byte, opts ...RequestOption) (dr *payload.DiffResponse, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/diff/%x/%x/", sum1, sum2), nil, nil, opts...)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	dr = &payload.DiffResponse{}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, dr)
	if err != nil {
		return nil, err
	}
	return dr, nil
}

func (c *Client) GetBlocks(sum []byte, start, end int, format payload.BlockFormat, opts ...RequestOption) (resp *http.Response, err error) {
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
	var qs string
	if len(v) > 0 {
		qs = fmt.Sprintf("?%s", v.Encode())
	}
	return c.Request(http.MethodGet, fmt.Sprintf("/tables/%x/blocks/%s", sum, qs), nil, nil, opts...)
}

func (c *Client) GetRows(sum []byte, offsets []int, opts ...RequestOption) (resp *http.Response, err error) {
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
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	cr = &payload.Commit{}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, cr)
	if err != nil {
		return nil, err
	}
	return cr, nil
}

func (c *Client) GetTable(sum []byte, opts ...RequestOption) (tr *payload.GetTableResponse, err error) {
	resp, err := c.Request(http.MethodGet, fmt.Sprintf("/tables/%x/", sum), nil, nil, opts...)
	if err != nil {
		return
	}
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	tr = &payload.GetTableResponse{}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, tr)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func (c *Client) sendUploadPackRequest(wants, haves [][]byte, done bool, opts ...RequestOption) (resp *http.Response, err error) {
	req := &payload.UploadPackRequest{}
	for _, want := range wants {
		req.Wants = payload.AppendHex(req.Wants, want)
	}
	for _, have := range haves {
		req.Haves = payload.AppendHex(req.Haves, have)
	}
	req.Done = done
	b, err := json.Marshal(req)
	if err != nil {
		return
	}
	return c.Request(http.MethodPost, "/upload-pack/", bytes.NewReader(b), map[string]string{
		"Content-Type": CTJSON,
	}, opts...)
}

func parseUploadPackResult(r io.ReadCloser) (acks [][]byte, err error) {
	defer r.Close()
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}
	resp := &payload.UploadPackResponse{}
	err = json.Unmarshal(b, resp)
	if err != nil {
		return
	}
	for _, h := range resp.ACKs {
		acks = append(acks, (*h)[:])
	}
	return
}

func (c *Client) PostUploadPack(wants, haves [][]byte, done bool, opts ...RequestOption) (acks [][]byte, pr *encoding.PackfileReader, err error) {
	resp, err := c.sendUploadPackRequest(wants, haves, done, opts...)
	if err != nil {
		return
	}
	if resp.Header.Get("Content-Type") == CTPackfile {
		pr, err = encoding.NewPackfileReader(resp.Body)
	} else if resp.Header.Get("Content-Type") == CTJSON {
		acks, err = parseUploadPackResult(resp.Body)
	}
	return
}

func (c *Client) PostUpdatesToReceivePack(updates map[string]*payload.Update, opts ...RequestOption) (*http.Response, error) {
	req := &payload.ReceivePackRequest{
		Updates: updates,
	}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	return c.Request(http.MethodPost, "/receive-pack/", bytes.NewReader(b), map[string]string{
		"Content-Type": CTJSON,
	}, opts...)
}

func (c *Client) ErrHTTP(resp *http.Response) error {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
}
