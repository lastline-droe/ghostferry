package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/siddontang/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

const (
	// These should be kept in sync with ghostferry.rb
	portEnvName string        = "GHOSTFERRY_INTEGRATION_PORT"
	timeout     time.Duration = 30 * time.Second
)

const (
	// These should be kept in sync with ghostferry.rb

	// Could only be sent once by the main thread
	StatusReady                  string = "READY"
	StatusBinlogStreamingStarted string = "BINLOG_STREAMING_STARTED"
	StatusBinlogStreamingStopped string = "BINLOG_STREAMING_STOPPED"
	StatusRowCopyCompleted       string = "ROW_COPY_COMPLETED"
	StatusVerifyDuringCutover    string = "VERIFY_DURING_CUTOVER"
	StatusVerified               string = "VERIFIED"
	StatusDone                   string = "DONE"

	// Could be sent by multiple goroutines in parallel
	StatusBeforeRowCopy     string = "BEFORE_ROW_COPY"
	StatusAfterRowCopy      string = "AFTER_ROW_COPY"
	StatusBeforeBinlogApply string = "BEFORE_BINLOG_APPLY"
	StatusAfterBinlogApply  string = "AFTER_BINLOG_APPLY"
)

type IntegrationFerry struct {
	*ghostferry.Ferry
}

// =========================================
// Code for integration server communication
// =========================================

func (f *IntegrationFerry) SendStatusAndWaitUntilContinue(status string, data ...string) error {
	integrationPort := os.Getenv(portEnvName)
	if integrationPort == "" {
		return fmt.Errorf("environment variable %s must be specified", portEnvName)
	}

	client := &http.Client{
		Timeout: timeout,
	}

	resp, err := client.PostForm(fmt.Sprintf("http://localhost:%s", integrationPort), url.Values{
		"status": []string{status},
		"data":   data,
	})

	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("server returned invalid status: %d", resp.StatusCode)
	}

	return nil
}

// Method override for Start in order to send status to the integration
// server.
func (f *IntegrationFerry) Start() error {
	f.Ferry.DataIterator.AddBatchListener(func(rowBatch ghostferry.RowBatch) error {
		return f.SendStatusAndWaitUntilContinue(StatusBeforeRowCopy, rowBatch.TableSchema().Name)
	})

	f.Ferry.BinlogStreamer.AddEventListener(func(event *ghostferry.ReplicationEvent) error {
		return f.SendStatusAndWaitUntilContinue(StatusBeforeBinlogApply)
	})

	err := f.Ferry.Start()
	if err != nil {
		return err
	}

	f.Ferry.DataIterator.AddBatchListener(func(rowBatch ghostferry.RowBatch) error {
		return f.SendStatusAndWaitUntilContinue(StatusAfterRowCopy, rowBatch.TableSchema().Name)
	})

	f.Ferry.BinlogStreamer.AddEventListener(func(event *ghostferry.ReplicationEvent) error {
		// the integration tests currently don't need any events other than row
		// updates. If we stream any query events, it becomes very tricky to
		// correctly handle events (any query would show up)
		if _, ok := event.BinlogEvent.Event.(*replication.RowsEvent); ok {
			return f.SendStatusAndWaitUntilContinue(StatusAfterBinlogApply)
		}
		return nil
	})

	return nil
}

// ===========================================
// Code to handle an almost standard Ferry run
// ===========================================
func (f *IntegrationFerry) Main() error {
	var err error

	err = f.SendStatusAndWaitUntilContinue(StatusReady)
	if err != nil {
		return err
	}

	err = f.Initialize()
	if err != nil {
		return err
	}

	err = f.Start()
	if err != nil {
		return err
	}

	err = f.SendStatusAndWaitUntilContinue(StatusBinlogStreamingStarted)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		f.Run()
	}()

	f.WaitUntilRowCopyIsComplete()
	err = f.SendStatusAndWaitUntilContinue(StatusRowCopyCompleted)
	if err != nil {
		return err
	}

	// TODO: this method should return errors rather than calling
	// the error handler to panic directly.
	f.FlushBinlogAndStopStreaming()
	err = f.SendStatusAndWaitUntilContinue(StatusBinlogStreamingStopped)
	if err != nil {
		return err
	}
	wg.Wait()

	if f.Verifier != nil {
		err := f.SendStatusAndWaitUntilContinue(StatusVerifyDuringCutover)
		if err != nil {
			return err
		}

		result, err := f.Verifier.VerifyDuringCutover()
		if err != nil {
			return err
		}

		// We now send the results back to the integration server as each verifier
		// might log them differently, making it difficult to assert that the
		// incorrect table was caught from the logs
		err = f.SendStatusAndWaitUntilContinue(StatusVerified, result.IncorrectTables...)
		if err != nil {
			return err
		}
	}

	return f.SendStatusAndWaitUntilContinue(StatusDone)
}

func NewStandardConfig() (*ghostferry.Config, error) {
	config := &ghostferry.Config{
		Source: &ghostferry.DatabaseConfig{
			Host:      "127.0.0.1",
			Port:      uint16(29291),
			User:      "root",
			Pass:      "",
			Collation: "utf8mb4_unicode_ci",
			Params: map[string]string{
				"charset": "utf8mb4",
			},
		},

		Target: &ghostferry.DatabaseConfig{
			Host:      "127.0.0.1",
			Port:      uint16(29292),
			User:      "root",
			Pass:      "",
			Collation: "utf8mb4_unicode_ci",
			Params: map[string]string{
				"charset": "utf8mb4",
			},
		},

		AutomaticCutover: true,
		FailOnFirstTableCopyError: true,
		TableFilter: &testhelpers.TestTableFilter{
			DbsFunc:    testhelpers.DbApplicabilityFilter([]string{"gftest"}),
			TablesFunc: nil,
		},

		DumpStateOnSignal: true,
		MyServerId:        99499,
	}

	integrationPort := os.Getenv(portEnvName)
	if integrationPort == "" {
		return nil, fmt.Errorf("environment variable %s must be specified", portEnvName)
	}

	config.ProgressCallback = ghostferry.HTTPCallback{
		URI: fmt.Sprintf("http://localhost:%s/callbacks/progress", integrationPort),
	}
	config.ProgressReportFrequency = 500

	resumeStateJSON, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return nil, err
	}

	if len(resumeStateJSON) > 0 {
		config.StateToResumeFrom = &ghostferry.SerializableState{}
		err = json.Unmarshal(resumeStateJSON, config.StateToResumeFrom)
		if err != nil {
			return nil, err
		}
	}

	verifierType := os.Getenv("GHOSTFERRY_VERIFIER_TYPE")
	if verifierType == ghostferry.VerifierTypeIterative {
		config.VerifierType = ghostferry.VerifierTypeIterative
		config.IterativeVerifierConfig = ghostferry.IterativeVerifierConfig{
			Concurrency: 2,
		}
	} else if verifierType != "" {
		config.VerifierType = verifierType
	}

	if resumeStateFromDB := os.Getenv("GHOSTFERRY_RESUMESTATEFROMDB"); resumeStateFromDB != "" {
		config.ResumeStateFromDB = resumeStateFromDB
		config.ForceResumeStateUpdatesToDB = true
	}

	if cascadingPaginationKeyColumnConfig := os.Getenv("GHOSTFERRY_CASCADING_PAGINATION_COLUMN_CONFIG"); cascadingPaginationKeyColumnConfig != "" {
		config.CascadingPaginationColumnConfig = &ghostferry.CascadingPaginationColumnConfig{}
		err = json.Unmarshal([]byte(cascadingPaginationKeyColumnConfig), config.CascadingPaginationColumnConfig)
		if err != nil {
			return nil, err
		}
	}

	if replicateSchemaChanges := os.Getenv("GHOSTFERRY_REPLICATESCHEMACHANGES"); replicateSchemaChanges != "" {
		err = json.Unmarshal([]byte(replicateSchemaChanges), &config.ReplicateSchemaChanges)
		if err != nil {
			return nil, err
		}
	}

	if delayDataIterationUntilBinlogWriterShutdown := os.Getenv("GHOSTFERRY_DELAYDATAITERATIONUNTILBINLOGWRITERSHUTDOWN"); delayDataIterationUntilBinlogWriterShutdown != "" {
		err = json.Unmarshal([]byte(delayDataIterationUntilBinlogWriterShutdown), &config.DelayDataIterationUntilBinlogWriterShutdown)
		if err != nil {
			return nil, err
		}
	}

	if iterateInDescendingOrder := os.Getenv("GHOSTFERRY_ITERATE_IN_DESCENDING_ORDER"); iterateInDescendingOrder != "" {
		err = json.Unmarshal([]byte(iterateInDescendingOrder), &config.IterateInDescendingOrder)
		if err != nil {
			return nil, err
		}
	}

	if lockStrategy := os.Getenv("GHOSTFERRY_LOCK_STRATEGY"); lockStrategy != "" {
		config.LockStrategy = lockStrategy
	}

	return config, config.ValidateConfig()
}

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.DebugLevel)

	config, err := NewStandardConfig()
	if err != nil {
		panic(err)
	}

	// This is currently a hack to customize the Ghostferry configuration.
	// TODO: allow Ghostferry config to be specified by the ruby test directly.
	compressedDataColumn := os.Getenv("GHOSTFERRY_DATA_COLUMN_SNAPPY")
	if compressedDataColumn != "" {
		config.CompressedColumnsForVerification = map[string]map[string]map[string]string{
			"gftest": map[string]map[string]string{
				"test_table_1": map[string]string{
					"data": "SNAPPY",
				},
			},
		}
	}

	ignoredColumn := os.Getenv("GHOSTFERRY_IGNORED_COLUMN")
	if ignoredColumn != "" {
		config.IgnoredColumnsForVerification = map[string]map[string]map[string]struct{}{
			"gftest": map[string]map[string]struct{}{
				"test_table_1": map[string]struct{}{
					ignoredColumn: struct{}{},
				},
			},
		}
	}

	f := &IntegrationFerry{
		Ferry: &ghostferry.Ferry{
			Config: config,
		},
	}

	integrationPort := os.Getenv(portEnvName)
	if integrationPort == "" {
		panic(fmt.Sprintf("environment variable %s must be specified", portEnvName))
	}

	f.ErrorHandler = &ghostferry.PanicErrorHandler{
		Ferry: f.Ferry,
		ErrorCallback: ghostferry.HTTPCallback{
			URI: fmt.Sprintf("http://localhost:%s/callbacks/error", integrationPort),
		},
		DumpState: true,
	}

	err = f.Main()
	if err != nil {
		panic(err)
	}
}
