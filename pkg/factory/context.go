package factory

import (
	"bytes"
	"context"
)

type parentsKey struct{}

func setParents(ctx context.Context, parents [][]byte) context.Context {
	return context.WithValue(ctx, parentsKey{}, parents)
}

func getParents(ctx context.Context) [][]byte {
	if v := ctx.Value(parentsKey{}); v != nil {
		return v.([][]byte)
	}
	return nil
}

type bufKey struct{}

func getBuffer(ctx context.Context) (*bytes.Buffer, context.Context) {
	if v := ctx.Value(bufKey{}); v != nil {
		return v.(*bytes.Buffer), ctx
	}
	buf := bytes.NewBuffer(nil)
	return buf, context.WithValue(ctx, bufKey{}, buf)
}
