package storage

import (
	"context"
	"strings"
	"testing"
)

func TestInvalidConfigurationDoesNotExposeCredentials(t *testing.T) {
	const marker = "sensitive-credential"
	_, pgErr := NewPostgres(context.Background(), "postgres://user:"+marker+"%xx@localhost/db")
	_, redisErr := NewRedis(context.Background(), "redis://user:"+marker+"%xx@localhost/0")
	for name, err := range map[string]error{"postgres": pgErr, "redis": redisErr} {
		if err == nil {
			t.Errorf("%s accepted invalid URL", name)
		} else if strings.Contains(err.Error(), marker) {
			t.Errorf("%s parser error exposed credential", name)
		}
	}
}
