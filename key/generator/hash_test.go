package generator

import (
	"crypto/md5"
	"testing"

	"github.com/meltwater/drone-cache/test"
)

func TestGenerateHash(t *testing.T) {
	t.Parallel()

	actual, err := NewHash(md5.New).Generate("hash")
	test.Ok(t, err)

	expected := "0800fc577294c34e0b28ad2839435945"
	test.Equals(t, actual, expected)
}
