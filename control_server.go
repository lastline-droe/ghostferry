package ghostferry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

type ControlServer struct {
	F        *Ferry
	Verifier Verifier
	Addr     string
	Basedir  string

	server    *http.Server
	logger    *logrus.Entry
	router    *mux.Router
	templates *template.Template
}

func (this *ControlServer) Initialize() (err error) {
	this.logger = logrus.WithField("tag", "control_server")
	this.logger.Info("initializing")

	this.router = mux.NewRouter()
	this.router.HandleFunc("/", this.HandleIndex).Methods("GET")
	this.router.HandleFunc("/api/actions/pause", this.HandlePause).Queries("type", "{type:migration|replication}").Methods("POST")
	this.router.HandleFunc("/api/actions/unpause", this.HandleUnpause).Queries("type", "{type:migration|replication}").Methods("POST")
	this.router.HandleFunc("/api/actions/cutover", this.HandleCutover).Queries("type", "{type:automatic|manual}").Methods("POST")
	this.router.HandleFunc("/api/actions/stop", this.HandleStop).Methods("POST")
	this.router.HandleFunc("/api/actions/verify", this.HandleVerify).Methods("POST")
	this.router.HandleFunc("/api/health", this.HandleStatusHealthCheck).Methods("GET")

	if WebUiBasedir != "" {
		this.Basedir = WebUiBasedir
	}

	staticFiles := http.StripPrefix("/static/", http.FileServer(http.Dir(filepath.Join(this.Basedir, "webui", "static"))))
	this.router.PathPrefix("/static/").Handler(staticFiles)

	this.templates, err = template.New("").ParseFiles(filepath.Join(this.Basedir, "webui", "index.html"))

	if err != nil {
		return err
	}

	this.server = &http.Server{
		Addr:    this.Addr,
		Handler: this,
	}

	return nil
}

func (this *ControlServer) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	this.logger.Infof("running on %s", this.Addr)
	err := this.server.ListenAndServe()
	if err != nil {
		logrus.WithError(err).Error("error on ListenAndServe")
	}
}

func (this *ControlServer) Shutdown() error {
	return this.server.Shutdown(nil)
}

func (this *ControlServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	this.router.ServeHTTP(w, r)

	this.logger.WithFields(logrus.Fields{
		"method": r.Method,
		"path":   r.RequestURI,
		"time":   time.Now().Sub(start),
	}).Info("served http request")
}

func (this *ControlServer) HandleIndex(w http.ResponseWriter, r *http.Request) {
	status := FetchStatusDeprecated(this.F, this.Verifier)

	err := this.templates.ExecuteTemplate(w, "index.html", status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (this *ControlServer) getThrottlerForRequest(w http.ResponseWriter, r *http.Request) Throttler {
	vars := mux.Vars(r)
	throttlerName := vars["type"]

	logger := this.logger.WithFields(logrus.Fields{
		"method":    r.Method,
		"path":      r.RequestURI,
		"throttler": throttlerName,
	})
	logger.Info("received http request for throttler")

	if throttlerName == "migration" {
		return this.F.MigrationThrottler
	} else if throttlerName == "replication" {
		return this.F.ReplicationThrottler
	} else {
		logger.Warning("received http request for unknown throttler")
		return nil
	}
}

func (this *ControlServer) HandlePause(w http.ResponseWriter, r *http.Request) {
	throttler := this.getThrottlerForRequest(w, r)
	if throttler == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	throttler.SetPaused(true)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleUnpause(w http.ResponseWriter, r *http.Request) {
	throttler := this.getThrottlerForRequest(w, r)
	if throttler == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	throttler.SetPaused(false)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleCutover(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if vars["type"] == "automatic" {
		this.F.AutomaticCutover = true
	} else if vars["type"] == "manual" {
		this.F.AutomaticCutover = false
	} else {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleStop(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (this *ControlServer) HandleVerify(w http.ResponseWriter, r *http.Request) {
	if this.Verifier == nil {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	err := this.Verifier.StartInBackground()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleStatusHealthCheck(w http.ResponseWriter, r *http.Request) {
	status := FetchStatusDeprecated(this.F, this.Verifier)

	// we allow the caller to specify conditions for which we return an HTTP-500 instead of an
	// HTTP-200, to allow built-in evaluation of the state. This is typically used by lifeness
	// probes in kubernetes
	reportAsError := false

	BinlogWriterStateTsAgeMs := r.FormValue("BinlogWriterStateTsAgeMs")
	if BinlogWriterStateTsAgeMs != "" {
		max, err := strconv.ParseInt(BinlogWriterStateTsAgeMs, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid BinlogWriterStateTsAgeMs: %s", err.Error()), http.StatusBadRequest)
			return
		}
		// time.Duration is signed, so we make "max" signed too - we have to check for 0 anyways
		if max <= 0 {
			http.Error(w, "Invalid BinlogWriterStateTsAgeMs: must be > 0", http.StatusBadRequest)
			return
		}
		reportAsError = max > 0 && status.BinlogWriterStateTsAge.Milliseconds() > max
	}

	statusAsJson, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if reportAsError {
		http.Error(w, bytes.NewBuffer(statusAsJson).String(), http.StatusInternalServerError)
	} else {
		w.Write(statusAsJson)
	}
}
