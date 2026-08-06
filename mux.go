package rmux

import (
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

type Config struct {
	Version int

	KeepAliveDisabled bool

	KeepAliveInterval time.Duration

	KeepAliveTimeout time.Duration

	MaxFrameSize int

	MaxStreamBuffer int

	StreamReadTimeout time.Duration

	IdleProbeAfter   time.Duration
	IdleProbeTimeout time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		Version:           1,
		KeepAliveInterval: 10 * time.Second,
		KeepAliveTimeout:  30 * time.Second,
		MaxFrameSize:      32768,
		MaxStreamBuffer:   65536,
		StreamReadTimeout: 2 * time.Second,
		IdleProbeAfter:    15 * time.Second,
		IdleProbeTimeout:  time.Second,
	}
}

func VerifyConfig(config *Config) error {
	if !(config.Version == 1 || config.Version == 2) {
		return errors.New("unsupported protocol version")
	}
	if !config.KeepAliveDisabled {
		if config.KeepAliveInterval == 0 {
			return errors.New("keep-alive interval must be positive")
		}
		if config.KeepAliveTimeout < config.KeepAliveInterval {
			return fmt.Errorf("keep-alive timeout must be larger than keep-alive interval")
		}
	}
	if config.MaxFrameSize <= 0 {
		return errors.New("max frame size must be positive")
	}
	if config.MaxFrameSize > 65535 {
		return errors.New("max frame size must not be larger than 65535")
	}
	if config.MaxStreamBuffer <= 0 {
		return errors.New("max stream buffer must be positive")
	}
	if config.MaxStreamBuffer > math.MaxInt32 {
		return errors.New("max stream buffer cannot be larger than 2147483647")
	}
	return nil
}

func Server(conn io.ReadWriteCloser, config *Config) (*Session, error) {
	if config == nil {
		config = DefaultConfig()
	}
	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	return newSession(config, conn, false), nil
}

func Client(conn io.ReadWriteCloser, config *Config) (*Session, error) {
	if config == nil {
		config = DefaultConfig()
	}
	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	return newSession(config, conn, true), nil
}
