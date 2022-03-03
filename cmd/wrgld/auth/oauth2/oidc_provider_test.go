package authoauth2

import (
	"net/http"
	"net/http/httptest"

	apiserver "github.com/wrgl/wrgl/pkg/api/server"
)

type oidcProvider struct {
	ClientID     string
	ClientSecret string
	s            *httptest.Server
}

type oidcDiscoveryResponse struct {
	Issuer                            string   `json:"issuer,omitempty"`
	AuthorizationEndpoint             string   `json:"authorization_endpoint,omitempty"`
	DeviceAuthorizationEndpoint       string   `json:"device_authorization_endpoint,omitempty"`
	TokenEndpoint                     string   `json:"token_endpoint,omitempty"`
	UserinfoEndpoint                  string   `json:"userinfo_endpoint,omitempty"`
	RevocationEndpoint                string   `json:"revocation_endpoint,omitempty"`
	JwksURI                           string   `json:"jwks_uri,omitempty"`
	ResponseTypesSupported            []string `json:"response_types_supported,omitempty"`
	SubjectTypesSupported             []string `json:"subject_types_supported,omitempty"`
	IdTokenSigningAlgValuesSupported  []string `json:"id_token_signing_alg_values_supported,omitempty"`
	ScopesSupported                   []string `json:"scopes_supported,omitempty"`
	TokenEndpointAuthMethodsSupported []string `json:"token_endpoint_auth_methods_supported,omitempty"`
	ClaimsSupported                   []string `json:"claims_supported,omitempty"`
	CodeChallengeMethodsSupported     []string `json:"code_challenge_methods_supported,omitempty"`
	GrantTypesSupported               []string `json:"grant_types_supported,omitempty"`
}

func (p *oidcProvider) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		apiserver.WriteJSON(rw, &oidcDiscoveryResponse{
			Issuer:                p.s.URL,
			AuthorizationEndpoint: p.s.URL + "/protocol/openid-connect/auth",
			TokenEndpoint:         p.s.URL + "/protocol/openid-connect/token",
			UserinfoEndpoint:      p.s.URL + "/protocol/openid-connect/userinfo",
			JwksURI:               p.s.URL + "/protocol/openid-connect/certs",
			IdTokenSigningAlgValuesSupported: []string{
				"ES256",
				"none",
			},
		})
	}
}

func startOIDCProvider(clientID, clientSecret string) *oidcProvider {
	p := &oidcProvider{
		ClientID:     clientID,
		ClientSecret: clientSecret,
	}
	p.s = httptest.NewServer(p)
	return p
}

func (p *oidcProvider) Close() {
	p.s.Close()
}
