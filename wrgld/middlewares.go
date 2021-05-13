package main

import (
	"log"
	"net/http"
)

func logging(handler http.Handler) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.RequestURI())
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error: %v", r)
				http.Error(rw, "internal server error", http.StatusInternalServerError)
			}
		}()
		handler.ServeHTTP(rw, r)
	}
}
