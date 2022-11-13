package apiclient

import (
	"net/http"

	"github.com/wrgl/wrgl/pkg/pbar"
)

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

type requestProgressBarKey struct{}

func WithRequestProgressBar(barContainer pbar.Container, total int64, message string) RequestOption {
	return func(r *http.Request) {
		bar := barContainer.NewBar(total, message, pbar.UnitKiB)
		r.Body = bar.ProxyReader(r.Body)
	}
}
