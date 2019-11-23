package apiserver

import (
	"fmt"
	"net/http"
	"time"

	"github.com/BronOS/segdb/internal/pkg/segdb"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// APIServer ...
type APIServer struct {
	config    *Config
	segdb     *segdb.Segdb
	logger    *logrus.Logger
	router    *mux.Router
	startedAt time.Time
}

// New ...
func New(config *Config) *APIServer {
	return &APIServer{
		config: config,
		segdb:  segdb.New(segdb.NewMultiFileStorage(config.StoragePath)),
		logger: logrus.New(),
		router: mux.NewRouter(),
	}
}

// Start ...
func (s *APIServer) Start() error {
	s.startedAt = time.Now()

	if err := s.configureLogger(); err != nil {
		return err
	}

	s.logger.Info("Init DB...")
	if err := s.segdb.Load(); err != nil {
		return err
	}

	s.configureRouter()

	s.logger.Info(fmt.Sprintf("Listening on addr: %s", s.config.BindAddr))
	return http.ListenAndServe(s.config.BindAddr, s.router)
}

// Configure Logger ...
func (s *APIServer) configureLogger() error {
	level, err := logrus.ParseLevel(s.config.LogLevel)

	if err != nil {
		return err
	}

	s.logger.SetLevel(level)

	return nil
}

// Configure Router ...
func (s *APIServer) configureRouter() {
	s.router.HandleFunc("/ping", handlePing(s)).Methods(http.MethodGet)
	s.router.HandleFunc("/info", handleInfo(s)).Methods(http.MethodGet)
	s.router.HandleFunc("/reload", handleReload(s)).Methods(http.MethodGet)
	s.router.HandleFunc("/add", handleAdd(s)).Methods(http.MethodPost)
	s.router.HandleFunc("/publish", handlePublish(s)).Methods(http.MethodPost)
	s.router.HandleFunc("/get", handleGet(s)).Methods(http.MethodGet)
	s.router.HandleFunc("/getall", handleGetAll(s)).Methods(http.MethodGet)
	s.router.HandleFunc("/list", handleList(s)).Methods(http.MethodGet)
	s.router.HandleFunc("/query", handleQuery(s)).Methods(http.MethodGet)
	s.router.HandleFunc("/delete", handleDelete(s)).Methods(http.MethodDelete)
}
