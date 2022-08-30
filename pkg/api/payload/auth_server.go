package payload

type AuthServer struct {
	Type   string `json:"type"`
	Issuer string `json:"issuer"`
}
