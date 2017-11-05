package sse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewSse(t *testing.T) {
	b := newSSE()
	assert.NotNil(t, b)
	assert.NotNil(t, b.getNotifier())
}
