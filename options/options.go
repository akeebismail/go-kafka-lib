package options

import (
	"context"
	"errors"
	"strings"
)

type Options struct {
	ServiceName         string
	Address             string
	CertContent         string
	AuthenticationToken string
	Username            string
	Password            string
	ctx                 context.Context
}

type Option func(o *Options) error

func SetStoreName(name string) Option {
	return func(o *Options) error {
		sn := strings.TrimSpace(name)
		if sn == "" {
			return errors.New("invalid store name")
		}
		o.ServiceName = sn
		return nil
	}
}

func SetAddress(address string) Option {
	return func(o *Options) error {
		o.Address = address
		return nil
	}
}

func SetAuthorizationToken(token string) Option {
	return func(o *Options) error {
		o.AuthenticationToken = token
		return nil
	}
}

func SetUsername(username string) Option {
	return func(o *Options) error {
		o.Username = username
		return nil
	}
}

func SetPassword(password string) Option {
	return func(o *Options) error {
		o.Password = password
		return nil
	}
}

func SetContext(ctx context.Context) Option {
	return func(o *Options) error {
		if ctx == nil {
			return errors.New("invalid context")
		}

		o.ctx = ctx
		return nil
	}
}

func (o *Options) Context() context.Context {
	if o.ctx == nil {
		return context.Background()
	}

	return o.ctx
}
