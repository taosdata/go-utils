package util

import (
	"github.com/google/uuid"
	"strings"
)

func UUID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}
