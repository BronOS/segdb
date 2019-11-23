package segdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegment_Match(t *testing.T) {
	segment := getSegments(1)[0]

	assert.True(t, segment.Match(map[string]interface{}{
		"level": 1,
		"uvs":   1,
	}))
}
