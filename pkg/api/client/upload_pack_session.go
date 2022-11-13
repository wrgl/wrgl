// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiclient

import (
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/api/payload"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/encoding/packfile"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/ref"
)

const defaultHavesPerRoundTrip = 256

type UploadPackSession struct {
	c                 *Client
	db                objects.Store
	receiver          *apiutils.ObjectReceiver
	rs                ref.Store
	wants             [][]byte
	q                 *ref.CommitsQueue
	havesPerRoundTrip int
	depth             int
	opts              []RequestOption
	bar               pbar.Bar
	receiverOpts      []apiutils.ObjectReceiveOption
	stateFn           stateFn
}

type UploadPackOption func(ses *UploadPackSession)

func WithUploadPackHavesPerRoundTrip(n int) UploadPackOption {
	return func(ses *UploadPackSession) {
		ses.havesPerRoundTrip = n
	}
}

func WithUploadPackDepth(n int) UploadPackOption {
	return func(ses *UploadPackSession) {
		ses.depth = n
	}
}

func WithUploadPackProgressBar(bar pbar.Bar) UploadPackOption {
	return func(ses *UploadPackSession) {
		ses.bar = bar
	}
}

func WithUploadPackRequestOptions(opts ...RequestOption) UploadPackOption {
	return func(ses *UploadPackSession) {
		ses.opts = opts
	}
}

func WithUploadPackReceiverOptions(opts ...apiutils.ObjectReceiveOption) UploadPackOption {
	return func(ses *UploadPackSession) {
		ses.receiverOpts = opts
	}
}

func NewUploadPackSession(db objects.Store, rs ref.Store, c *Client, advertised [][]byte, opts ...UploadPackOption) (*UploadPackSession, error) {
	neg := &UploadPackSession{
		c:  c,
		db: db,
		rs: rs,
	}
	for _, opt := range opts {
		opt(neg)
	}
	if neg.havesPerRoundTrip == 0 {
		neg.havesPerRoundTrip = defaultHavesPerRoundTrip
	}
	for _, b := range advertised {
		if !objects.CommitExist(db, b) {
			neg.wants = append(neg.wants, b)
		}
	}
	if len(neg.wants) == 0 {
		return nil, fmt.Errorf("nothing wanted")
	}
	neg.receiver = apiutils.NewObjectReceiver(db, neg.wants, c.logger, neg.receiverOpts...)
	neg.stateFn = neg.negotiate
	return neg, nil
}

func (n *UploadPackSession) popHaves() (haves [][]byte, done bool, err error) {
	if n.q == nil {
		m, err := ref.ListAllRefs(n.rs)
		if err != nil {
			return nil, false, err
		}
		sl := [][]byte{}
		for _, v := range m {
			sl = append(sl, v)
		}
		n.q, err = ref.NewCommitsQueue(n.db, sl)
		if err != nil {
			return nil, false, err
		}
	}
	for i := 0; i < n.havesPerRoundTrip; {
		sum, com, err := n.q.PopInsertParents()
		if err == io.EOF {
			done = true
			break
		}
		if err != nil {
			return nil, false, err
		}
		if objects.TableExist(n.db, com.Table) {
			haves = append(haves, sum)
			i++
		}
	}
	return
}

func (n *UploadPackSession) negotiate() (stateFn, error) {
	haves, done, err := n.popHaves()
	if err != nil {
		return nil, fmt.Errorf("error poping haves: %w", err)
	}
	upr, pr, err := n.c.PostUploadPack(&payload.UploadPackRequest{
		Wants: payload.BytesSliceToHexSlice(n.wants),
		Haves: payload.BytesSliceToHexSlice(haves),
		Done:  done,
		Depth: n.depth,
	}, n.opts...)
	if err != nil {
		return nil, fmt.Errorf("error requesting upload pack (state=negotiate): %w", err)
	}
	n.wants = nil
	if pr != nil {
		return n.receiveObjects(pr)
	} else if len(upr.TableHaves) > 0 {
		return n.negotiateTables(upr)
	}
	if err = n.q.RemoveAncestors(payload.HexSliceToBytesSlice(upr.ACKs)); err != nil {
		return nil, err
	}
	return n.negotiate, nil
}

func (n *UploadPackSession) negotiateTables(upr *payload.UploadPackResponse) (stateFn, error) {
	acks := [][]byte{}
	for _, sum := range upr.TableHaves {
		b := (*sum)[:]
		if objects.TableExist(n.db, b) {
			acks = append(acks, b)
		}
	}
	upr, pr, err := n.c.PostUploadPack(&payload.UploadPackRequest{
		TableACKs: payload.BytesSliceToHexSlice(acks),
	}, n.opts...)
	if err != nil {
		return nil, fmt.Errorf("error requesting upload pack (state=negotiateTables): %w", err)
	}
	if pr != nil {
		return n.receiveObjects(pr)
	}
	return n.negotiateTables(upr)
}

func (n *UploadPackSession) receiveObjects(pr *packfile.PackfileReader) (stateFn, error) {
	var err error
	if pr == nil {
		_, pr, err = n.c.PostUploadPack(&payload.UploadPackRequest{}, n.opts...)
		if err != nil {
			return nil, fmt.Errorf("error requesting upload pack (state=receiveObjects): %w", err)
		}
	}
	defer pr.Close()
	doneReceiving, err := n.receiver.Receive(pr, n.bar)
	if err != nil {
		return nil, fmt.Errorf("error receiving objects: %w", err)
	}
	if doneReceiving {
		return nil, nil
	}
	return n.receiveObjects(nil)
}

func (n *UploadPackSession) Start() ([][]byte, error) {
	var err error
	for n.stateFn != nil {
		n.stateFn, err = n.stateFn()
		if err != nil {
			return nil, err
		}
	}
	return n.receiver.ReceivedCommits, nil
}
