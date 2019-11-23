package segdb

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

// StorageInterface ...
type StorageInterface interface {
	Save(segment *Segment) error
	Delete(id string) error
	Clear() error
	Load() (map[string]*Segment, error)
}

// MultiFileStorage ...
type MultiFileStorage struct {
	storagePath string
}

// NewMultiFileStorage ...
func NewMultiFileStorage(storagePath string) *MultiFileStorage {
	return &MultiFileStorage{storagePath: storagePath}
}

// Save ...
func (s *MultiFileStorage) Save(segment *Segment) error {
	segmentsJSON, err := json.Marshal(segment)
	if err != nil {
		return err
	}

	os.MkdirAll(s.storagePath, os.ModePerm)

	if err := ioutil.WriteFile(path.Join(s.storagePath, segment.ID+".json"), segmentsJSON, os.ModePerm); err != nil {
		return err
	}

	return nil
}

// Delete ...
func (s *MultiFileStorage) Delete(id string) error {
	if err := os.Remove(path.Join(s.storagePath, id+".json")); err != nil {
		return err
	}
	return nil
}

// Clear ...
func (s *MultiFileStorage) Clear() error {
	dir, err := filepath.Abs(s.storagePath)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}

	return nil
}

// Load ...
func (s *MultiFileStorage) Load() (map[string]*Segment, error) {
	segments := map[string]*Segment{}

	files, err := ioutil.ReadDir(s.storagePath)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		segmentsJSON, err := ioutil.ReadFile(path.Join(s.storagePath, f.Name()))
		if err != nil {
			return nil, err
		}

		segment := &Segment{}
		if err := json.Unmarshal(segmentsJSON, segment); err != nil {
			return nil, err
		}

		segments[segment.ID] = segment
	}

	return segments, nil
}
