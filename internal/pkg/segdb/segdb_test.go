package segdb

import (
	"os"
	"path"
	"strconv"
	"testing"
	"unsafe"

	"github.com/antonmedv/expr"
	"github.com/stretchr/testify/assert"
)

var (
	storagePath string = "../../../var/lib/segdb_test"
)

func TestSegdb_GetIndexSize(t *testing.T) {
	s := getSegDb()

	segment := getSegments(1)[0]

	x1 := unsafe.Sizeof(map[string]map[interface{}][]string{})
	x2 := s.GetIndexSize()
	assert.Equal(t, x1, x2)

	assert.NoError(t, s.Add(segment))
	assert.FileExists(t, path.Join(storagePath, segment.ID+".json"))
	assert.True(t, s.GetIndexSize() > x1)

	clearStorage()
}

func TestSegdb_GetSegmentsCount(t *testing.T) {
	s := getSegDb()

	segment := getSegments(1)[0]

	assert.NoError(t, s.Add(segment))
	assert.Equal(t, 1, s.GetSegmentsCount())

	clearStorage()
}

func TestSegdb_Add(t *testing.T) {
	s := getSegDb()

	segment := getSegments(1)[0]

	assert.NoError(t, s.Add(segment))
	assert.Equal(t, 1, s.GetSegmentsCount())

	clearStorage()
}

func TestSegdb_Publish(t *testing.T) {
	s := getSegDb()

	segment := getSegments(1)[0]

	assert.NoError(t, s.Publish([]*Segment{segment}))
	assert.Equal(t, 1, s.GetSegmentsCount())

	clearStorage()
}

func TestSegdb_Delete(t *testing.T) {
	s := getSegDb()

	segment := getSegments(1)[0]

	assert.NoError(t, s.Add(segment))
	assert.Equal(t, 1, s.GetSegmentsCount())

	assert.NoError(t, s.Delete(segment.ID))
	assert.Equal(t, 0, s.GetSegmentsCount())

	assert.Zero(t, s.GetSegmentsCount())

	clearStorage()
}

func TestSegdb_Get(t *testing.T) {
	s := getSegDb()

	segment := getSegments(1)[0]

	assert.NoError(t, s.Add(segment))
	assert.Equal(t, 1, s.GetSegmentsCount())

	_, ok := s.Get("seg")
	assert.Error(t, ok)

	seg, ok := s.Get(segment.ID)
	assert.NoError(t, ok)
	assert.Equal(t, segment.ID, seg.ID)

	clearStorage()
}

func TestSegdb_GetAll(t *testing.T) {
	s := getSegDb()

	segments := getSegments(5)
	s.Publish(segments)

	found := s.GetAll([]string{"seg1", "seg5"})

	assert.Len(t, found, 2)
	assert.Equal(t, segments[0].ID, found[0].ID)
	assert.Equal(t, segments[4].ID, found[1].ID)

	clearStorage()
}

func TestSegdb_List(t *testing.T) {
	s := getSegDb()

	segments := getSegments(5)
	for _, seg := range segments {
		assert.NoError(t, s.Add(seg))
	}

	found := s.List(map[string]interface{}{
		"idx1": 1,
		"idx2": "idx2_str",
	}, -1, -1)

	assert.Len(t, found, 5)
	assert.Equal(t, segments[0].ID, found[0].ID)
	assert.Equal(t, segments[1].ID, found[1].ID)
	assert.Equal(t, segments[2].ID, found[2].ID)
	assert.Equal(t, segments[3].ID, found[3].ID)
	assert.Equal(t, segments[4].ID, found[4].ID)

	found = s.List(map[string]interface{}{
		"idx1": 1,
		"idx2": "idx2_str",
	}, 1, 1)
	assert.Len(t, found, 1)
	assert.Equal(t, segments[1].ID, found[0].ID)

	found = s.List(map[string]interface{}{
		"idx1": 1,
		"idx2": "idx2_str",
	}, 2, 5)
	assert.Len(t, found, 0)

	found = s.List(map[string]interface{}{
		"idx1": 1,
		"idx2": "idx2_str",
	}, 2, 3)
	assert.Len(t, found, 2)

	clearStorage()
}

func TestSegdb_Query(t *testing.T) {
	s := getSegDb()

	segments := getSegments(5)
	for _, seg := range segments {
		assert.NoError(t, s.Add(seg))
	}

	segment := &Segment{
		ID:      "seg100",
		Data:    "test data",
		Filters: "level == 2 && uvs == 3 && dps == 4",
		Indexes: map[string]interface{}{
			"idx1": 2,
		},
	}
	program, _ := expr.Compile(segment.Filters)
	segment.Program = program
	assert.NoError(t, s.Add(segment))

	found := s.Query(map[string]interface{}{
		"idx1":  1,
		"idx2":  "idx2_str",
		"level": 1,
		"uvs":   1,
	}, 0)
	assert.Len(t, found, 5)
	assert.Equal(t, segments[0].ID, found[0].ID)
	assert.Equal(t, segments[1].ID, found[1].ID)
	assert.Equal(t, segments[2].ID, found[2].ID)
	assert.Equal(t, segments[3].ID, found[3].ID)
	assert.Equal(t, segments[4].ID, found[4].ID)

	found = s.Query(map[string]interface{}{
		"level": 1,
		"uvs":   1,
	}, 0)
	assert.Len(t, found, 5)

	found = s.Query(map[string]interface{}{
		"level": 1,
	}, 0)
	assert.Len(t, found, 0)

	found = s.Query(map[string]interface{}{
		"level": 2,
		"uvs":   3,
		"dps":   4,
	}, 0)
	assert.Len(t, found, 6)

	found = s.Query(map[string]interface{}{
		"level": 2,
		"uvs":   3,
		"dps":   4,
	}, 1)
	assert.Len(t, found, 1)

	found = s.Query(map[string]interface{}{
		"idx1":  2,
		"level": 2,
		"uvs":   3,
		"dps":   4,
	}, 0)
	assert.Len(t, found, 1)

	clearStorage()
}

func TestSegdb_IdIndex(t *testing.T) {
	s := getSegDb()

	segments := getSegments(2)

	segment1 := segments[0]
	segment2 := segments[1]

	assert.NoError(t, s.Add(segment1))
	assert.Equal(t, 1, s.GetSegmentsCount())
	assert.NoError(t, s.Add(segment2))
	assert.Equal(t, 2, s.GetSegmentsCount())

	assert.Len(t, s.idIndex, 2)
	assert.Equal(t, segment1.ID, s.idIndex[0])
	assert.Equal(t, segment2.ID, s.idIndex[1])

	assert.NoError(t, s.Delete(segment1.ID))
	assert.Len(t, s.idIndex, 1)
	assert.Equal(t, segment2.ID, s.idIndex[0])

	clearStorage()
}

// getSegments ...
func getSegments(l int) []*Segment {
	segments := []*Segment{}
	for i := 0; i < l; i++ {
		segment := &Segment{
			ID:      "seg" + strconv.Itoa((i + 1)),
			Data:    "test data",
			Filters: "level >= 1 && uvs in [1,2,3]",
			Indexes: map[string]interface{}{
				"idx1": 1,
				"idx2": "idx2_str",
			},
		}
		program, _ := expr.Compile(segment.Filters)
		segment.Program = program
		segments = append(segments, segment)
	}
	return segments
}

func getSegDb() *Segdb {
	return New(NewMultiFileStorage(storagePath))
}

func clearStorage() {
	os.RemoveAll(storagePath)
}
