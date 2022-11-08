package conf

type WebhookEventType string

const (
	CommitEventType    WebhookEventType = "commit"
	RefUpdateEventType WebhookEventType = "refUpdate"
)

type Webhook struct {
	// URL is the endpoint to send events
	URL string `json:"url" yaml:"url"`

	// EventTypes is the list of event types to send. Must not be empty
	EventTypes []WebhookEventType `json:"eventTypes" yaml:"eventTypes"`

	// when SecretToken is set, wrgld also send hmac-sha256 hex digest of the payload body
	// via header "X-Wrgl-Signature-256"
	SecretToken string `json:"secretToken,omitempty" yaml:"secretToken,omitempty"`
}
