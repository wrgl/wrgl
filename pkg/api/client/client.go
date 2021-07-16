// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"

	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/encoding"
	"golang.org/x/net/publicsuffix"
)

const (
	CTJSON     = "application/json"
	CTPackfile = "application/x-wrgl-packfile"
)

type Client struct {
	client *http.Client
	// origin is the scheme + host name of remote server
	origin string
}

func NewClient(origin string) (*Client, error) {
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return nil, err
	}
	return &Client{
		client: &http.Client{
			Jar: jar,
		},
		origin: origin,
	}, nil
}

func (c *Client) Request(method, path string, body io.Reader, headers map[string]string) (resp *http.Response, err error) {
	req, err := http.NewRequest(method, c.origin+path, body)
	if err != nil {
		return
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err = c.client.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode >= 400 {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%s: %s", resp.Status, string(b))
	}
	return
}

func (c *Client) GetRefs() (m map[string][]byte, err error) {
	resp, err := c.Request(http.MethodGet, "/refs/", nil, nil)
	if err != nil {
		return
	}
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	defer resp.Body.Close()
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

func (c *Client) sendUploadPackRequest(wants, haves [][]byte, done bool) (resp *http.Response, err error) {
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
	})
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

func (c *Client) PostUploadPack(wants, haves [][]byte, done bool) (acks [][]byte, pr *encoding.PackfileReader, err error) {
	resp, err := c.sendUploadPackRequest(wants, haves, done)
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

func (c *Client) PostUpdatesToReceivePack(updates map[string]*payload.Update) (*http.Response, error) {
	req := &payload.ReceivePackRequest{
		Updates: updates,
	}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	return c.Request(http.MethodPost, "/receive-pack/", bytes.NewReader(b), map[string]string{
		"Content-Type": CTJSON,
	})
}

func (c *Client) ErrHTTP(resp *http.Response) error {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
}

// func (c *Client) PostReceivePack(updates []*payload.Update, writeObjects func(w io.Writer) error) (body io.ReadCloser, err error) {
// 	reqBody := bytes.NewBuffer(nil)
// 	gzw := gzip.NewWriter(reqBody)
// 	buf := misc.NewBuffer(nil)
// 	strs := make([]string, 0, 3)
// 	sendPackfile := false
// 	for _, update := range updates {
// 		strs = strs[:0]
// 		for _, sum := range [][]byte{update.OldSum, update.Sum} {
// 			if sum == nil {
// 				strs = append(strs, strings.Repeat("0", 32))
// 			} else {
// 				strs = append(strs, hex.EncodeToString(sum))
// 			}
// 		}
// 		if update.Sum != nil {
// 			sendPackfile = true
// 		}
// 		strs = append(strs, update.Dst)
// 		err = encoding.WritePktLine(gzw, buf, strings.Join(strs, " "))
// 		if err != nil {
// 			return
// 		}
// 	}
// 	err = encoding.WritePktLine(gzw, buf, "")
// 	if err != nil {
// 		return
// 	}
// 	if sendPackfile {
// 		err = writeObjects(gzw)
// 		if err != nil {
// 			return
// 		}
// 	}
// 	err = gzw.Close()
// 	if err != nil {
// 		return
// 	}
// 	resp, err := c.Request(http.MethodPost, "/receive-pack/", reqBody, map[string]string{
// 		"Content-Type":     "application/x-wrgl-receive-pack-request",
// 		"Content-Encoding": "gzip",
// 	})
// 	if err != nil {
// 		return
// 	}
// 	return resp.Body, nil
// }
