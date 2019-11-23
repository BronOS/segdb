package apiserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/BronOS/segdb/internal/pkg/segdb"
	"net/http"
	"strconv"
	"time"
)

type appendRequest struct {
	ID      string                 `json:"id"`
	Data    string                 `json:"data,omitempty"`
	Filters string                 `json:"filters"`
	Indexes map[string]interface{} `json:"indexes,omitempty"`
}

// handlePing...
func handlePing(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		d := time.Since(s.startedAt)

		writeJSON(w, &map[string]interface{}{
			"status": "OK",
			"uptime": int64(d.Seconds()),
		})
	}
}

// handleInfo...
func handleInfo(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		d := time.Since(s.startedAt)

		writeJSON(w, &map[string]interface{}{
			"status":         "OK",
			"uptime":         int64(d.Seconds()),
			"index_size":     s.segdb.GetIndexSize(),
			"segments_count": s.segdb.GetSegmentsCount(),
		})
	}
}

// handleGet...
func handleGet(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ids, ok := r.URL.Query()["id"]

		if !ok || len(ids[0]) < 1 {
			s.logger.Error(errors.New("Bad Request. ID not found"))
			writeERRORCode(w, fmt.Errorf("ID not found"), http.StatusBadRequest)
			return
		}

		segment, err := s.segdb.Get(ids[0])
		if err != nil {
			s.logger.Error(err)
			writeERRORCode(w, fmt.Errorf("Not found"), http.StatusNotFound)
			return
		}

		writeJSON(w, &map[string]interface{}{
			"id":      segment.ID,
			"data":    segment.Data,
			"filters": segment.Filters,
			"indexes": segment.Indexes,
		})
	}
}

// handleDelete...
func handleDelete(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ids, ok := r.URL.Query()["id"]

		if !ok || len(ids[0]) < 1 {
			s.logger.Error(errors.New("Bad Request. ID not found"))
			writeERRORCode(w, fmt.Errorf("ID not found"), http.StatusBadRequest)
			return
		}

		if err := s.segdb.Delete(ids[0]); err != nil {
			s.logger.Error(err)
			writeERRORCode(w, fmt.Errorf("Not found"), http.StatusNotFound)
			return
		}

		writeJSON(w, &map[string]interface{}{
			"id": ids[0],
		})
	}
}

// handleGetAll...
func handleGetAll(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ids, ok := r.URL.Query()["id"]

		if !ok || len(ids[0]) < 1 {
			s.logger.Error(errors.New("Bad Request. IDs not found"))
			writeERRORCode(w, fmt.Errorf("IDs not found"), http.StatusBadRequest)
			return
		}

		segments := s.segdb.GetAll(ids)
		m := make([]*map[string]interface{}, len(segments))

		for _, segment := range segments {
			m = append(m, &map[string]interface{}{
				"id":      segment.ID,
				"data":    segment.Data,
				"filters": segment.Filters,
				"indexes": segment.Indexes,
			})
		}

		writeJSON(w, m)
	}
}

// handleList...
func handleList(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		limit := -1
		offset := -1
		indexes := map[string]interface{}{}

		for k, v := range params {
			if k == "limit" && len(v) > 0 {
				lim, err := strconv.Atoi(v[0])
				if err != nil {
					s.logger.Error(err)
					writeERRORCode(w, fmt.Errorf("Limit must be INT"), http.StatusBadRequest)
					return
				}
				limit = lim
				continue
			}

			if k == "offset" && len(v) > 0 {
				ofs, err := strconv.Atoi(v[0])
				if err != nil {
					s.logger.Error(err)
					writeERRORCode(w, fmt.Errorf("Offset must be INT"), http.StatusBadRequest)
					return
				}
				offset = ofs
				continue
			}

			// try to convert to FLOAT32
			f32v, err := strconv.ParseFloat(v[0], 32)
			if err == nil {
				indexes[k] = f32v
				continue
			}

			// try to convert to FLOAT64
			f64v, err := strconv.ParseFloat(v[0], 64)
			if err == nil {
				indexes[k] = f64v
				continue
			}

			// try to convert to INT
			iv, err := strconv.Atoi(v[0])
			if err == nil {
				indexes[k] = iv
				continue
			}

			// try to convert to BOOL
			bv, err := strconv.ParseBool(v[0])
			if err == nil {
				indexes[k] = bv
				continue
			}

			indexes[k] = v[0]
		}

		segments := s.segdb.List(indexes, limit, offset)
		m := []*map[string]interface{}{}

		for _, segment := range segments {
			m = append(m, &map[string]interface{}{
				"id":      segment.ID,
				"data":    segment.Data,
				"filters": segment.Filters,
				"indexes": segment.Indexes,
			})
		}

		writeJSON(w, m)
	}
}

// handleQuery...
func handleQuery(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		limit := -1
		p := map[string]interface{}{}

		for k, v := range params {
			if k == "limit" && len(v) > 0 {
				lim, err := strconv.Atoi(v[0])
				if err != nil {
					s.logger.Error(err)
					writeERRORCode(w, fmt.Errorf("Limit must be INT"), http.StatusBadRequest)
					return
				}
				limit = lim
				continue
			}

			// try to convert to FLOAT32
			f32v, err := strconv.ParseFloat(v[0], 32)
			if err == nil {
				p[k] = f32v
				continue
			}

			// try to convert to FLOAT64
			f64v, err := strconv.ParseFloat(v[0], 64)
			if err == nil {
				p[k] = f64v
				continue
			}

			// try to convert to INT
			iv, err := strconv.Atoi(v[0])
			if err == nil {
				p[k] = iv
				continue
			}

			// try to convert to BOOL
			bv, err := strconv.ParseBool(v[0])
			if err == nil {
				p[k] = bv
				continue
			}

			p[k] = v[0]
		}

		segments := s.segdb.Query(p, limit)
		m := []*map[string]interface{}{}

		for _, segment := range segments {
			m = append(m, &map[string]interface{}{
				"id":      segment.ID,
				"data":    segment.Data,
				"filters": segment.Filters,
				"indexes": segment.Indexes,
			})
		}

		writeJSON(w, m)
	}
}

// handleReload...
func handleReload(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.segdb.Load(); err != nil {
			s.logger.Error(err)
			writeERROR(w, errors.New("err text"))
		}
	}
}

// handleAppend...
func handleAdd(s *APIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := &appendRequest{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.logger.Error(err)
			writeERRORCode(w, fmt.Errorf("Bad Request"), http.StatusBadRequest)
			return
		}

		if err := s.segdb.Add(&segdb.Segment{
			ID:      req.ID,
			Data:    req.Data,
			Filters: req.Filters,
			Indexes: req.Indexes,
		}); err != nil {
			s.logger.Error(err)
			writeERROR(w, err)
		}
	}
}

// handlePublish...
func handlePublish(s *APIServer) http.HandlerFunc {
	type request []struct {
		appendRequest
	}

	return func(w http.ResponseWriter, r *http.Request) {
		reqSlice := &request{}
		if err := json.NewDecoder(r.Body).Decode(reqSlice); err != nil {
			s.logger.Error(err)
			writeERRORCode(w, fmt.Errorf("Bad Request"), http.StatusBadRequest)
			return
		}

		segments := []*segdb.Segment{}
		for _, req := range *reqSlice {
			segments = append(segments, &segdb.Segment{
				ID:      req.ID,
				Data:    req.Data,
				Filters: req.Filters,
				Indexes: req.Indexes,
			})
		}

		if err := s.segdb.Publish(segments); err != nil {
			s.logger.Error(err)
			writeERRORCode(w, err, http.StatusBadRequest)
		}
	}
}

///////////////////////////////////////////////////////////////////////

// writeJSONCode ...
func writeJSONCode(w http.ResponseWriter, data interface{}, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

// writeJSON ...
func writeJSON(w http.ResponseWriter, data interface{}) {
	writeJSONCode(w, data, http.StatusOK)
}

// writeERRORCode ...
func writeERRORCode(w http.ResponseWriter, err error, code int) {
	writeJSONCode(w, map[string]interface{}{
		"status": "ERR",
		"error":  err.Error(),
	}, code)
}

// writeERROR ...
func writeERROR(w http.ResponseWriter, err error) {
	writeERRORCode(w, err, http.StatusInternalServerError)
}
