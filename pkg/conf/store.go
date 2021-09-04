package conf

type Store interface {
	Open() (*Config, error)
	Save(*Config) error
}
