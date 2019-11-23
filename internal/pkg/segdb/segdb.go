package segdb

import (
	"errors"
	"unsafe"

	"github.com/antonmedv/expr"
)

var (
	// ErrNotFound segemnt not  found
	ErrNotFound = errors.New("not found")
	// ErrReservedIndex reserved index
	ErrReservedIndex = errors.New("reserved index")
)

// Segdb ...
type Segdb struct {
	storage  StorageInterface
	indexes  map[string]map[interface{}][]string
	segments map[string]*Segment
	idIndex  []string
}

// New ...
func New(storage StorageInterface) *Segdb {
	return &Segdb{
		storage:  storage,
		indexes:  make(map[string]map[interface{}][]string),
		segments: make(map[string]*Segment),
		idIndex:  []string{},
	}
}

// Query ...
func (s *Segdb) Query(m map[string]interface{}, limit int) []*Segment {
	segments := []*Segment{}
	indexes := map[string]interface{}{}

	// find indexes in map
	for idxName, idxValue := range m {
		if _, ok := s.indexes[idxName]; ok == true {
			indexes[idxName] = idxValue
			delete(m, idxName)
		}
	}

	// unlimited
	if limit < 1 || limit > len(s.segments) {
		limit = len(s.segments)
	}

	// match segment to map and return EXIT flag in case when limit has been achived
	match := func(segment *Segment) bool {
		if segment.Match(m) {
			segments = append(segments, segment)
			if len(segments) >= limit {
				return true
			}
		}
		return false
	}

	if len(indexes) > 0 {
		for _, segment := range s.List(indexes, -1, -1) {
			if match(segment) == true {
				return segments
			}
		}
	} else {
		for _, segment := range s.segments {
			if match(segment) == true {
				return segments
			}
		}
	}

	return segments
}

// Publish ...
func (s *Segdb) Publish(m []*Segment) error {
	processed := make(map[string]*Segment, len(m))
	if err := s.storage.Clear(); err != nil {
		return err
	}

	for _, segment := range m {
		program, err := expr.Compile(segment.Filters)
		if err != nil {
			if err := s.storage.Clear(); err != nil {
				return err
			}
			return err
		}
		segment.Program = program

		if err := s.storage.Save(segment); err != nil {
			if err := s.storage.Clear(); err != nil {
				return err
			}
			return err
		}

		processed[segment.ID] = segment
	}

	s.segments = processed
	s.Reindex()

	return nil
}

// Load ...
func (s *Segdb) Load() error {
	segments, err := s.storage.Load()

	if err != nil {
		return nil
	}

	s.segments = segments

	s.Reindex()

	return nil
}

// Get ...
func (s *Segdb) Get(id string) (*Segment, error) {
	segment, ok := s.segments[id]

	if ok == false {
		return nil, ErrNotFound
	}

	return segment, nil
}

// GetAll ...
func (s *Segdb) GetAll(ids []string) []*Segment {
	segments := []*Segment{}

	for _, id := range ids {
		if seg, err := s.Get(id); err == nil {
			segments = append(segments, seg)
		}
	}

	return segments
}

// List ...
func (s *Segdb) List(indexes map[string]interface{}, limit int, offset int) []*Segment {
	ids := []string{}

	if len(indexes) > 0 {
		for idx, v := range indexes {
			idxs, ok := s.indexes[idx]
			if ok == true {
				iids, ok := idxs[v]
				if ok == true {
					ids = append(ids, iids...)
				}
			}
		}
		ids = s.unique(ids)
	} else {
		ids = append(ids, s.idIndex...)
	}

	if limit < 1 || limit > len(ids) {
		limit = len(ids)
	}

	if offset < 0 {
		offset = 0
	}

	if offset > len(ids) {
		offset = len(ids)
	}

	ids = ids[offset : offset+limit]

	return s.GetAll(ids)
}

func (s *Segdb) unique(intSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range intSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

// Delete ...
func (s *Segdb) Delete(id string) error {
	if _, ok := s.segments[id]; ok == false {
		return ErrNotFound
	}

	if err := s.storage.Delete(id); err != nil {
		return err
	}

	s.RemoveFromIndexes(id)
	delete(s.segments, id)

	return nil
}

// Add ...
func (s *Segdb) Add(segment *Segment) error {
	program, err := expr.Compile(segment.Filters)
	if err != nil {
		return err
	}
	segment.Program = program

	if err := s.storage.Save(segment); err != nil {
		return err
	}

	s.segments[segment.ID] = segment

	s.Index(segment, true)

	return nil
}

// Index ...
func (s *Segdb) Index(segment *Segment, clear bool) {
	if clear {
		s.RemoveFromIndexes(segment.ID)
	}

	for id, i := range segment.Indexes {
		if _, ok := s.indexes[id]; ok == false {
			s.indexes[id] = make(map[interface{}][]string)
		}

		if _, ok := s.isIndexExists(s.indexes[id][i], segment.ID); ok == false {
			s.indexes[id][i] = append(s.indexes[id][i], segment.ID)
		}
	}

	if _, ok := s.isIndexExists(s.idIndex, segment.ID); ok == false {
		s.idIndex = append(s.idIndex, segment.ID)
	}
}

// Reindex ...
func (s *Segdb) Reindex() {
	s.indexes = make(map[string]map[interface{}][]string)
	s.idIndex = []string{}

	for _, segment := range s.segments {
		s.Index(segment, false)
	}
}

// RemoveFromIndexes ...
func (s *Segdb) RemoveFromIndexes(id string) {
	for index, m := range s.indexes {
		for value, l := range m {
			for i, e := range l {
				if e == id {
					s.indexes[index][value] = append(l[:i], l[i+1:]...)
					break
				}
			}
		}
	}

	for idx, segID := range s.idIndex {
		if segID == id {
			s.idIndex = append(s.idIndex[:idx], s.idIndex[idx+1:]...)
		}
	}
}

// isIndexExists ...
func (s *Segdb) isIndexExists(index []string, id string) (int, bool) {
	for idx, seg := range index {
		if seg == id {
			return idx, true
		}
	}
	return -1, false
}

// GetIndexSize ...
func (s *Segdb) GetIndexSize() uintptr {
	size := unsafe.Sizeof(s.indexes)

	for k, v := range s.indexes {
		size += unsafe.Sizeof(k)
		for idx, id := range v {
			size += unsafe.Sizeof(idx) + unsafe.Sizeof(id)
		}
	}

	return size
}

// GetSegmentsCount ...
func (s *Segdb) GetSegmentsCount() int {
	return len(s.segments)
}
