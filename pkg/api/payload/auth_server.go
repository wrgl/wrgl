package payload

type AuthServer struct {
	Type       string `json:"type"`
	Issuer     string `json:"issuer"`
	ResourceID string `json:"resourceId"`
	Audience   string `json:"audience"`
}
