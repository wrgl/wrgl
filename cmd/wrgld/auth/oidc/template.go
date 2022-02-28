package authoidc

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
)

var (
	//go:embed templates static
	contentFS embed.FS

	deviceTmpl         *template.Template
	deviceLoggedInTmpl *template.Template
	errorTmpl          *template.Template
)

func init() {
	var err error
	deviceTmpl, err = template.ParseFS(contentFS, "templates/device.html", "templates/head.html")
	if err != nil {
		panic(err)
	}
	deviceLoggedInTmpl, err = template.ParseFS(contentFS, "templates/device-logged-in.html", "templates/head.html")
	if err != nil {
		panic(err)
	}
	errorTmpl, err = template.ParseFS(contentFS, "templates/error.html", "templates/head.html")
	if err != nil {
		panic(err)
	}
}

type headTmplData struct {
	Title string
}

type baseTmplData struct {
	Head *headTmplData
}

type deviceTmplData struct {
	baseTmplData

	ErrorMessage string
}

type deviceLoggedInTmplData struct {
	baseTmplData
}

type errorTmplData struct {
	baseTmplData

	ErrorMessage string
}

func writeDeviceHTML(rw http.ResponseWriter, data *deviceTmplData) {
	data.Head = &headTmplData{
		Title: "Device Log In",
	}
	writeHTML(rw, deviceTmpl, data)
}

func writeDeviceLoggedInHTML(rw http.ResponseWriter, data *deviceLoggedInTmplData) {
	data.Head = &headTmplData{
		Title: "Device Logged In",
	}
	writeHTML(rw, deviceLoggedInTmpl, data)
}

func writeErrorHTML(rw http.ResponseWriter, status int, data *errorTmplData) {
	data.Head = &headTmplData{
		Title: fmt.Sprintf("Error: %s", data.ErrorMessage),
	}
	rw.WriteHeader(status)
	writeHTML(rw, errorTmpl, data)
}
