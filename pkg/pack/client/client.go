// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packclient

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"strings"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"golang.org/x/net/publicsuffix"
)

type Update struct {
	OldSum []byte
	Sum    []byte
	Src    string
	Dst    string
	Force  bool
	ErrMsg string
}

type Client struct {
	client *http.Client
	// origin is the scheme + host name of remote server
	origin string
	db     kv.DB
	fs     kv.FileStore
}

func NewClient(db kv.DB, fs kv.FileStore, origin string) (*Client, error) {
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return nil, err
	}
	return &Client{
		client: &http.Client{
			Jar: jar,
		},
		origin: origin,
		db:     db,
		fs:     fs,
	}, nil
}

func (c *Client) request(method, path string, body io.Reader, headers map[string]string) (resp *http.Response, err error) {
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

func (c *Client) GetRefsInfo() (m map[string][]byte, err error) {
	resp, err := c.request(http.MethodGet, "/info/refs/", nil, nil)
	if err != nil {
		return
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-wrgl-upload-pack-advertisement" {
		return nil, fmt.Errorf("unrecognized content type: %q", ct)
	}
	defer resp.Body.Close()
	m = map[string][]byte{}
	parser := encoding.NewParser(resp.Body)
	var s string
	for {
		s, err = encoding.ReadPktLine(parser)
		if err != nil {
			return
		}
		if s == "" {
			break
		}
		b := make([]byte, 16)
		_, err = hex.Decode(b, []byte(s[:32]))
		if err != nil {
			return
		}
		m[s[33:]] = b
	}
	return
}

func (c *Client) sendUploadPackRequest(wants, haves [][]byte, done bool) (resp *http.Response, err error) {
	body := bytes.NewBuffer(nil)
	buf := misc.NewBuffer(nil)
	for _, want := range wants {
		err = encoding.WritePktLine(body, buf, "want "+hex.EncodeToString(want))
		if err != nil {
			return
		}
	}
	for _, have := range haves {
		err = encoding.WritePktLine(body, buf, "have "+hex.EncodeToString(have))
		if err != nil {
			return
		}
	}
	if done {
		err = encoding.WritePktLine(body, buf, "done")
	} else {
		err = encoding.WritePktLine(body, buf, "")
	}
	if err != nil {
		return
	}
	return c.request(http.MethodPost, "/upload-pack/", body, map[string]string{
		"Content-Type": "application/x-wrgl-upload-pack-request",
	})
}

func (c *Client) sendReceivePackRequest(updates []*Update, commits []*objects.Commit, commonCommits [][]byte) (resp *http.Response, err error) {
	body := bytes.NewBuffer(nil)
	gzw := gzip.NewWriter(body)
	buf := misc.NewBuffer(nil)
	strs := make([]string, 0, 3)
	sendPackfile := false
	for _, update := range updates {
		strs = strs[:0]
		for i, sum := range [][]byte{update.OldSum, update.Sum} {
			if sum == nil {
				strs = append(strs, strings.Repeat("0", 32))
			} else {
				strs = append(strs, hex.EncodeToString(sum))
				if i == 1 {
					sendPackfile = true
				}
			}
		}
		strs = append(strs, update.Dst)
		err = encoding.WritePktLine(gzw, buf, strings.Join(strs, " "))
		if err != nil {
			return
		}
	}
	err = encoding.WritePktLine(gzw, buf, "")
	if err != nil {
		return
	}
	if sendPackfile {
		err = packutils.WriteCommitsToPackfile(c.db, c.fs, commits, commonCommits, gzw)
		if err != nil {
			return
		}
	}
	err = gzw.Flush()
	if err != nil {
		return
	}
	return c.request(http.MethodPost, "/receive-pack/", body, map[string]string{
		"Content-Type":     "application/x-wrgl-receive-pack-request",
		"Content-Encoding": "gzip",
	})
}

func parseUploadPackResult(r io.ReadCloser) (acks [][]byte, err error) {
	defer r.Close()
	parser := encoding.NewParser(r)
	var s string
	for {
		s, err = encoding.ReadPktLine(parser)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(s, "ACK ") {
			b := make([]byte, 16)
			_, err = hex.Decode(b, []byte(s[4:]))
			if err != nil {
				return nil, err
			}
			acks = append(acks, b)
		} else if strings.HasPrefix(s, "NAK") {
			break
		}
	}
	return
}

func (c *Client) PostUploadPack(wants, haves [][]byte, done bool) (acks [][]byte, pr *encoding.PackfileReader, err error) {
	resp, err := c.sendUploadPackRequest(wants, haves, done)
	if err != nil {
		return
	}
	if resp.Header.Get("Content-Type") == "application/x-wrgl-packfile" {
		pr, err = encoding.NewPackfileReader(resp.Body)
	} else if resp.Header.Get("Content-Type") == "application/x-wrgl-upload-pack-result" {
		acks, err = parseUploadPackResult(resp.Body)
	}
	return
}

func (c *Client) parseReceivePackResult(r io.ReadCloser) (status map[string]string, err error) {
	defer r.Close()
	parser := encoding.NewParser(r)
	s, err := encoding.ReadPktLine(parser)
	if err != nil {
		return
	}
	if !strings.HasPrefix(s, "unpack ") {
		err = fmt.Errorf("unrecognized payload: %q", s)
		return
	}
	if s[7:] != "ok" {
		err = fmt.Errorf("unpack error: %s", s[7:])
		return
	}
	status = map[string]string{}
	for {
		s, err = encoding.ReadPktLine(parser)
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		if s == "" {
			break
		}
		if strings.HasPrefix(s, "ok ") {
			status[s[3:]] = ""
		} else if strings.HasPrefix(s, "ng ") {
			i := bytes.LastIndexByte([]byte(s), ' ')
			status[s[3:i]] = s[i:]
		} else {
			err = fmt.Errorf("unrecognized payload: %q", s)
			return
		}
	}
	return
}

func (c *Client) PostReceivePack(updates []*Update, commits []*objects.Commit, commonCommits [][]byte) (err error) {
	resp, err := c.sendReceivePackRequest(updates, commits, commonCommits)
	if err != nil {
		return
	}
	status, err := c.parseReceivePackResult(resp.Body)
	if err != nil {
		return
	}
	for _, u := range updates {
		if s, ok := status[u.Dst]; ok {
			u.ErrMsg = s
		} else {
			u.ErrMsg = "no status reported"
		}
	}
	return
}
