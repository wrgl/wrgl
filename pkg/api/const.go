// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api

const (
	CTCSV          = "text/csv"
	CTJSON         = "application/json"
	CTBlocksBinary = "application/x-wrgl-blocks-binary"
	PathCommit     = "/commits/"
)
const PathRefs = "/refs/"
const PathReceivePack = "/receive-pack/"

const (
	ReceivePackSessionCookie = "receive-pack-session-id"
	CTReceivePackResult      = "application/x-wrgl-receive-pack-result"
)

const PathUploadPack = "/upload-pack/"

const (
	// UploadPackSessionCookie identifies upload-pack session
	UploadPackSessionCookie = "upload-pack-session-id"

	// CTUploadPackResult is content type for upload pack result
	CTUploadPackResult = "application/x-wrgl-upload-pack-result"

	// CTPackfile is content type for packfile
	CTPackfile = "application/x-wrgl-packfile"

	// PurgeUploadPackSessionHeader is a trailer header that is set to "true"
	// at the end of an upload pack session. Upon seeing this any in-between proxy
	// should purge sticky session related to UploadPackSessionCookie
	PurgeUploadPackSessionHeader = "Wrgl-Purge-Upload-Pack-Session"
)
