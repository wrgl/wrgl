package conf

import "time"

type Duration time.Duration

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalText(data []byte) error {
	o, err := time.ParseDuration(string(data))
	if err != nil {
		return err
	}
	*d = Duration(o)
	return nil
}
