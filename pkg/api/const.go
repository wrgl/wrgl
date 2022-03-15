// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package api

const (
	// CTCSV is content type for CSV payload
	CTCSV = "text/csv"
	// CTJSON is content type for Json payload
	CTJSON = "application/json"
	// CTUploadPackResult is content type for upload pack result payload
	CTUploadPackResult = "application/x-wrgl-upload-pack-result"
	// CTPackfile is content type for packfile payload
	CTPackfile = "application/x-wrgl-packfile"
	// CTBlocksBinary is content type for blocks binary payload
	CTBlocksBinary = "application/x-wrgl-blocks-binary"
	// CTReceivePackResult is content type for receive pack result payload
	CTReceivePackResult = "application/x-wrgl-receive-pack-result"

	// CookieReceivePackSession identifies receive-pack session
	CookieReceivePackSession = "receive-pack-session-id"
	// CookieUploadPackSession identifies upload-pack session
	CookieUploadPackSession = "upload-pack-session-id"

	// HeaderPurgeUploadPackSession is a trailer header that is set to "true"
	// at the end of an upload pack session. Upon seeing this any in-between proxy
	// should purge sticky session related to CookieUploadPackSession
	HeaderPurgeUploadPackSession = "Wrgl-Purge-Upload-Pack-Session"

	PathCommit      = "/commits/"
	PathRefs        = "/refs/"
	PathReceivePack = "/receive-pack/"
	PathUploadPack  = "/upload-pack/"
)
