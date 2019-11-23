package apiserver

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	storagePath string = "../../../var/lib/segdb_test"
)

func Test_handlePing(t *testing.T) {
	s := getAPIServer()
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/ping", nil)
	handlePing(s).ServeHTTP(rec, req)

	assert.Equal(t, 200, rec.Code)
}

func Test_handleInfo(t *testing.T) {
	s := getAPIServer()
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/info", nil)
	handleInfo(s).ServeHTTP(rec, req)

	assert.Equal(t, 200, rec.Code)
}

func Test_handleList(t *testing.T) {
	s := getAPIServer()
	s.segdb.Load()
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/list", nil)
	handleList(s).ServeHTTP(rec, req)

	response := rec.Body.String()

	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "OK", response)

	// m := make(map[string]interface{})
	// json.NewDecoder()
}

func getAPIServer() *APIServer {
	return New(&Config{
		LogLevel:    "debug",
		StoragePath: "../../../var/lib/segdb",
		BindAddr:    ":4510",
	})
}

func clearStorage() {
	os.RemoveAll(storagePath)
}
