package auth

type AuthnStore interface {
	SetPassword(email, password string) error
	CheckPassword(email, password string) bool
	RemoveUser(email string) error
}
