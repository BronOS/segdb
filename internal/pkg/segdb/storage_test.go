package segdb

import (
	"encoding/json"
	"os"
	"path"
	"testing"

	"github.com/antonmedv/expr"
	"github.com/stretchr/testify/assert"
)

func TestMultiFileStorage_Delete(t *testing.T) {
	segment := &Segment{
		ID:      "seg1",
		Data:    "test data",
		Filters: "level >= 1 && uvs in [1,2,3]",
		Indexes: map[string]interface{}{
			"idx1": 1,
			"idx2": "idx2_str",
		},
	}

	program, _ := expr.Compile(segment.Filters)
	segment.Program = program

	storagePath := "../../../var/lib/segdb_test"
	fileNme := segment.ID + ".json"
	filePath := path.Join(storagePath, fileNme)

	s := &MultiFileStorage{
		storagePath: storagePath,
	}

	assert.NoError(t, s.Save(segment), "MultiFileStorage.Delete() saving failed")
	assert.FileExistsf(t, filePath, "MultiFileStorage.Delete() file not exists")

	assert.NoError(t, s.Delete(segment.ID), "MultiFileStorage.Delete() file not deleted")
	_, err := os.Stat(filePath)
	assert.True(t, os.IsNotExist(err), "MultiFileStorage.Delete() file exists")

	os.RemoveAll(storagePath)
}

func TestMultiFileStorage_Save(t *testing.T) {
	segment := &Segment{
		ID:      "seg1",
		Data:    "test data",
		Filters: "level >= 1 && uvs in [1,2,3]",
		Indexes: map[string]interface{}{
			"idx1": 1,
			"idx2": "idx2_str",
		},
	}

	program, _ := expr.Compile(segment.Filters)
	segment.Program = program

	storagePath := "../../../var/lib/segdb_test"

	s := &MultiFileStorage{
		storagePath: storagePath,
	}

	assert.NoError(t, s.Save(segment), "MultiFileStorage.Save() saving failed")
	assert.FileExistsf(t, path.Join(storagePath, segment.ID+".json"), "MultiFileStorage.Save() file not exists")

	os.RemoveAll(storagePath)
}

func TestMultiFileStorage_Clear(t *testing.T) {
	segment := &Segment{
		ID:      "seg1",
		Data:    "test data",
		Filters: "level >= 1 && uvs in [1,2,3]",
		Indexes: map[string]interface{}{
			"idx1": 1,
			"idx2": "idx2_str",
		},
	}

	program, _ := expr.Compile(segment.Filters)
	segment.Program = program

	storagePath := "../../../var/lib/segdb_test"
	fileNme := segment.ID + ".json"
	filePath := path.Join(storagePath, fileNme)

	s := &MultiFileStorage{
		storagePath: storagePath,
	}

	assert.NoError(t, s.Save(segment), "MultiFileStorage.Clear() saving failed")
	assert.FileExistsf(t, filePath, "MultiFileStorage.Clear() file not exists")

	assert.NoError(t, s.Clear(), "MultiFileStorage.Clear() clearing failed")
	_, err := os.Stat(filePath)
	assert.True(t, os.IsNotExist(err), "MultiFileStorage.Clear() file exists")

	os.RemoveAll(storagePath)
}

func TestMultiFileStorage_Load(t *testing.T) {
	indexes := make(map[string]interface{})
	json.Unmarshal([]byte("{'idx1': 1, 'idx2', ''idx2_str}"), &indexes)
	segment := &Segment{
		ID:      "seg1",
		Data:    "test data",
		Filters: "level >= 1 && uvs in [1,2,3]",
		Indexes: indexes,
	}

	program, _ := expr.Compile(segment.Filters)
	segment.Program = program

	storagePath := "../../../var/lib/segdb_test"
	fileNme := segment.ID + ".json"
	filePath := path.Join(storagePath, fileNme)

	defer os.RemoveAll(storagePath)

	s := &MultiFileStorage{
		storagePath: storagePath,
	}

	assert.NoError(t, s.Save(segment), "MultiFileStorage.Clear() saving failed")
	assert.FileExistsf(t, filePath, "MultiFileStorage.Clear() file not exists")

	segments, err := s.Load()

	assert.NoError(t, err)
	assert.Len(t, segments, 1)
	assert.Equal(t, segments[segment.ID].ID, segment.ID)
	assert.Equal(t, segments[segment.ID].Data, segment.Data)
	assert.Equal(t, segments[segment.ID].Filters, segment.Filters)
	assert.Equal(t, segments[segment.ID].Indexes, segment.Indexes)

	output1, _ := expr.Run(segment.Program, map[string]int{"level": 2, "uvs": 2})
	output2, _ := expr.Run(segments[segment.ID].Program, map[string]int{"level": 2, "uvs": 2})

	assert.Equal(t, output1, output2)
}
