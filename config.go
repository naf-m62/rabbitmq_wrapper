package wrapper

import (
	"fmt"
	"net/url"
)

type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func (c *Config) Url() string {
	var u = &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
	}
	return u.String()
}
