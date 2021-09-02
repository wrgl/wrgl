package auth

const (
	ActRead       = "read"
	ActReadConfig = "readConfig"
	ActWrite      = "write"
)

type AuthzStore interface {
	AddPolicy(email, act string) error
	RemovePolicy(email, act string) error
	Authorized(email, act string) (bool, error)
}
