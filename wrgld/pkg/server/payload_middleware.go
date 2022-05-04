package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type payloadRecorderKey struct{}

type payloadRecorder struct {
	requestInfo  interface{}
	responseInfo interface{}
}

func setPayloadRecorder(r *http.Request, pr *payloadRecorder) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), payloadRecorderKey{}, pr))
}

func getPayloadRecorder(r *http.Request) *payloadRecorder {
	if v := r.Context().Value(payloadRecorderKey{}); v != nil {
		return v.(*payloadRecorder)
	}
	return nil
}

func setRequestInfo(r *http.Request, info interface{}) {
	if v := getPayloadRecorder(r); v != nil {
		v.requestInfo = info
	}
}

func setResponseInfo(r *http.Request, info interface{}) {
	if v := getPayloadRecorder(r); v != nil {
		v.responseInfo = info
	}
}

func PayloadMiddleware() func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pr := &payloadRecorder{}
			handler.ServeHTTP(w, setPayloadRecorder(r, pr))
			ts := time.Now().Format(time.RFC3339)
			if pr.requestInfo != nil {
				b, err := json.MarshalIndent(pr.requestInfo, "    ", "  ")
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s %s %s\n  request %s\n", ts, r.Method, r.URL, string(b))

				if pr.responseInfo != nil {
					b, err := json.MarshalIndent(pr.responseInfo, "    ", "  ")
					if err != nil {
						panic(err)
					}
					fmt.Printf("  response %s\n", string(b))
				}
			}
		})
	}
}
