package sse

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestNewSse(t *testing.T) {
	b := newSSE()
	assert.NotNil(t, b)
	assert.NotNil(t, b.getNotifier())
}
