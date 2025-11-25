package pinottest

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

import _ "embed"

//go:embed data/*
var TestDataFS embed.FS

const (
	Timeout      = 5 * time.Minute
	PollInterval = 1 * time.Second

	ControllerUrl = "http://localhost:9000"
	BrokerUrl     = "http://localhost:8000"

	InfraMetricsTableName       = "infraMetrics"
	BenchmarkTableName          = "benchmark"
	PartialTableName            = "partial"
	NginxLogsTableName          = "nginxLogs"
	DerivedTimeBucketsTableName = "derivedTimeBuckets"
	EmptyTableName              = "empty"
)

type CreateTableJob struct {
	tableName  string
	schemaFile string
	configFile string
	dataFile   string
}

var createTestTablesOnce sync.Once
var testTableJobs []CreateTableJob

func CreateTestTables() {
	jobs := []CreateTableJob{{
		tableName:  InfraMetricsTableName,
		schemaFile: "data/infraMetrics_schema.json",
		configFile: "data/infraMetrics_offline_table_config.json",
		dataFile:   "data/infraMetrics_data.json",
	}, {
		tableName:  BenchmarkTableName,
		schemaFile: "data/benchmark_schema.json",
		configFile: "data/benchmark_offline_table_config.json",
		dataFile:   "data/benchmark_data.json",
	}, {
		tableName:  PartialTableName,
		schemaFile: "data/partial_schema.json",
		configFile: "data/partial_offline_table_config.json",
		dataFile:   "data/partial_data_1.json",
	}, {
		tableName:  NginxLogsTableName,
		schemaFile: "data/nginxLogs_schema.json",
		configFile: "data/nginxLogs_offline_table_config.json",
		dataFile:   "data/nginxLogs_data.json",
	}, {
		tableName:  DerivedTimeBucketsTableName,
		schemaFile: "data/derivedTimeBuckets_schema.json",
		configFile: "data/derivedTimeBuckets_offline_table_config.json",
	}, {
		tableName:  EmptyTableName,
		schemaFile: "data/empty_schema.json",
		configFile: "data/empty_offline_table_config.json",
	}, {
		tableName:  "allDataTypes",
		schemaFile: "data/allDataTypes_schema.json",
		configFile: "data/allDataTypes_offline_table_config.json",
		dataFile:   "data/allDataTypes_data.json",
	}, {
		tableName:  "hourlyEvents",
		schemaFile: "data/hourlyEvents_schema.json",
		configFile: "data/hourlyEvents_offline_table_config.json",
	}, {
		tableName:  "timeSeriesWithMapLabels",
		schemaFile: "data/timeSeriesWithMapLabels_schema.json",
		configFile: "data/timeSeriesWithMapLabels_offline_table_config.json",
		dataFile:   "data/timeSeriesWithMapLabels_data.json",
	}, {
		tableName:  "nginxLogsWithJson",
		schemaFile: "data/nginxLogsWithJson_schema.json",
		configFile: "data/nginxLogsWithJson_offline_table_config.json",
		dataFile:   "data/nginxLogsWithJson_data.json",
	}, {
		tableName:  "nginxLogsWithMap",
		schemaFile: "data/nginxLogsWithMap_schema.json",
		configFile: "data/nginxLogsWithMap_offline_table_config.json",
		dataFile:   "data/nginxLogsWithMap_data.json",
	}, {
		tableName:  "nullValues",
		schemaFile: "data/nullValues_schema.json",
		configFile: "data/nullValues_offline_table_config.json",
		dataFile:   "data/nullValues_data.json",
	}}

	// Store jobs globally for validation on subsequent calls
	testTableJobs = jobs

	// One-time initialization: wait for Pinot to be ready
	createTestTablesOnce.Do(func() {
		WaitForPinot(Timeout)
	})

	// Validate/recreate tables every time this function is called
	// First pass: check which tables need recreation (serially to avoid overwhelming Pinot)
	var tablesToRecreate []CreateTableJob
	for _, job := range jobs {
		if tableExists(job.tableName) {
			// For tables with data files, verify ALL segments are GOOD
			if job.dataFile != "" {
				segments := listSegmentStatusForTable(job.tableName)
				goodSegments := 0
				for _, status := range segments {
					if status.SegmentStatus == "GOOD" {
						goodSegments++
					}
				}
				// Check if ALL segments are GOOD
				if len(segments) > 0 && goodSegments == len(segments) {
					continue // Table is healthy, skip
				}
				// Table needs recreation
				fmt.Printf("Table %s has %d/%d GOOD segments, will recreate...\n", job.tableName, goodSegments, len(segments))
				tablesToRecreate = append(tablesToRecreate, job)
			}
			// else: Table exists and doesn't need data, skip
		} else {
			// Table doesn't exist, needs to be created
			fmt.Printf("Table %s doesn't exist, will create...\n", job.tableName)
			tablesToRecreate = append(tablesToRecreate, job)
		}
	}

	if len(tablesToRecreate) == 0 {
		return // All tables are healthy
	}

	// Second pass: recreate tables serially to avoid overwhelming Pinot in CI
	var somethingChanged atomic.Bool
	setupTable := func(job CreateTableJob) {
		// Delete if exists
		if tableExists(job.tableName) {
			fmt.Printf("Deleting table %s...\n", job.tableName)
			deleteTable(job.tableName)
			waitForTableDeletion(job.tableName, Timeout)
			// Delete schema only after table is fully deleted to avoid 409 conflict
			deleteTableSchema(job.tableName)
		}

		fmt.Printf("Creating table %s...\n", job.tableName)
		somethingChanged.Store(true)
		// Schema was already deleted if table existed, only delete if it still exists
		if schemaExists(job.tableName) {
			deleteTableSchema(job.tableName)
		}
		createTableSchema(job.schemaFile)
		waitForTableSchema(job.tableName, Timeout)
		createTableConfig(job.configFile)
		// Wait for table to be fully initialized and ready to accept data
		waitForTableReady(job.tableName, Timeout)

		if job.dataFile == "" {
			return
		}
		uploadJsonTableData(job.tableName+"_OFFLINE", job.dataFile)
		WaitForSegmentsAllGood(job.tableName, Timeout)

		// Delete the partial table's segment and upload a new segment
		if job.tableName == PartialTableName {
			uploadJsonTableData(PartialTableName+"_OFFLINE", "data/partial_data_2.json")
			WaitForSegmentsAllGood(job.tableName, Timeout)
			
			// Wait a bit more to ensure both segments are fully registered
			time.Sleep(3 * time.Second)
			
			segments := listOfflineSegments(job.tableName)
			if len(segments) < 2 {
				// If we don't have 2 segments yet, wait a bit more and try again
				fmt.Printf("Warning: Expected 2 segments but got %d, waiting and retrying...\n", len(segments))
				time.Sleep(5 * time.Second)
				segments = listOfflineSegments(job.tableName)
			}
			
			if len(segments) != 2 {
				panic(fmt.Sprintf("expected 2 segments but got %d after retries", len(segments)))
			}
			deleteSegmentFromFilesystem(segments[0])
			resetSegments(PartialTableName)
			waitForSegmentStatus(PartialTableName, segments[0], "BAD", Timeout)
		}
	}

	// Recreate tables serially (not in parallel) to avoid overwhelming Pinot
	for _, job := range tablesToRecreate {
		setupTable(job)
	}

	if somethingChanged.Load() {
		fmt.Println("Pinot setup complete.")
	}
}

func WaitForSegmentsAllGood(tableName string, timeout time.Duration) {
	pollTicker := time.NewTicker(PollInterval)
	defer pollTicker.Stop()

	timeoutTicker := time.NewTimer(timeout)
	defer timeoutTicker.Stop()

	for {
		statuses := listSegmentStatusForTable(tableName)
		goodSegments := 0
		for _, status := range statuses {
			if status.SegmentStatus == "GOOD" {
				goodSegments++
			}
		}
		if len(statuses) == goodSegments && len(statuses) > 0 {
			return
		}

		select {
		case <-timeoutTicker.C:
			panic(fmt.Sprintf("Timed out waiting for segments for %s", tableName))
		case <-pollTicker.C:
		}
	}
}

func waitForSegmentStatus(tableName string, segmentName string, segmentStatus string, timeout time.Duration) {
	pollTicker := time.NewTicker(PollInterval)
	defer pollTicker.Stop()

	timeoutTicker := time.NewTimer(timeout)
	defer timeoutTicker.Stop()

	for {
		statuses := listSegmentStatusForTable(tableName)
		for _, status := range statuses {
			if status.SegmentName == segmentName && status.SegmentStatus == segmentStatus {
				return
			}
		}

		select {
		case <-timeoutTicker.C:
			panic(fmt.Sprintf("Timed out waiting for %s segment status to %s", segmentName, segmentStatus))
		case <-pollTicker.C:
		}
	}
}

func listOfflineSegments(tableName string) []string {
	req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/segments/"+tableName, nil)
	requireNoError(err)
	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)

	requireNoError(err)
	requireOkStatus(resp)
	var data []struct {
		Offline []string `json:"OFFLINE"`
	}
	requireNoError(json.NewDecoder(resp.Body).Decode(&data))
	if len(data) != 1 {
		panic(fmt.Sprintf("Expected 1 result, got %d %v", len(data), data))
	}
	return data[0].Offline
}

type SegmentStatus struct {
	SegmentName   string `json:"segmentName"`
	SegmentStatus string `json:"segmentStatus"`
}

func listSegmentStatusForTable(tableName string) []SegmentStatus {
	req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/tables/"+tableName+"/segmentsStatus", nil)
	requireNoError(err)
	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)
	
	// Return empty list if table doesn't exist yet (during creation)
	if resp.StatusCode == http.StatusNotFound {
		return []SegmentStatus{}
	}
	
	requireOkStatus(resp)
	var data []SegmentStatus
	requireNoError(json.NewDecoder(resp.Body).Decode(&data))
	return data
}

func resetSegments(tableName string) {
	req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/segments/"+tableName+"_OFFLINE/reset?errorSegmentsOnly=false", nil)
	requireNoError(err)

	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)
	requireOkStatus(resp)
}

func deleteSegmentFromFilesystem(segmentName string) {
	cmd := exec.Command("docker", "compose", "exec", "pinot",
		"find", "/tmp", "-name", segmentName, "-exec", "rm", "-rf", "{}", "+")
	fmt.Println("Executing: ", cmd.String())
	err := cmd.Run()
	requireNoError(err)
}

func requireNoError(err error) {
	if err != nil {
		panic(err)
	}
}

func WaitForPinot(timeout time.Duration) {
	pollTicker := time.NewTicker(PollInterval)
	defer pollTicker.Stop()

	timeoutTicker := time.NewTimer(timeout)
	defer timeoutTicker.Stop()

	isReady := func() bool {
		req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/instances", nil)
		req.Header.Set("Accept", "application/json")
		requireNoError(err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer safeClose(resp.Body)

		if resp.StatusCode != http.StatusOK {
			return false
		}
		var respData struct {
			Instances []string `json:"instances"`
		}
		requireNoError(json.NewDecoder(resp.Body).Decode(&respData))

		var hasController bool
		var hasBroker bool
		var hasServer bool
		for _, instance := range respData.Instances {
			switch {
			case strings.HasPrefix(instance, "Controller"):
				hasController = true
			case strings.HasPrefix(instance, "Broker"):
				hasBroker = true
			case strings.HasPrefix(instance, "Server"):
				hasServer = true
			}
		}
		return hasController && hasBroker && hasServer
	}

	if isReady() {
		return
	}
	fmt.Println("Waiting for Pinot...")
	for {
		select {
		case <-timeoutTicker.C:
			panic("Timed out waiting for Pinot")
		case <-pollTicker.C:
			if isReady() {
				return
			}
		}
	}
}

func schemaExists(schemaName string) bool {
	req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/schemas/"+schemaName, nil)
	requireNoError(err)

	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		panic(fmt.Sprintf("Unexpected status code: %d", resp.StatusCode))
	}
	return resp.StatusCode == http.StatusOK
}

func createTableSchema(schemaFile string) {
	var body bytes.Buffer
	multipartWriter := multipart.NewWriter(&body)
	defer safeClose(multipartWriter)

	formWriter, err := multipartWriter.CreateFormFile("schemaName", schemaFile)
	requireNoError(err)

	file, err := TestDataFS.Open(schemaFile)
	requireNoError(err)
	defer safeClose(file)

	_, err = io.Copy(formWriter, file)
	requireNoError(err)

	safeClose(multipartWriter)
	req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/schemas", &body)
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)
	requireOkStatus(resp)
}

func deleteTableSchema(schemaName string) {
	// Retry logic to handle race conditions where table might still be associated
	maxRetries := 3
	retryDelay := 2 * time.Second
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodDelete, ControllerUrl+"/schemas/"+schemaName, nil)
		resp, err := http.DefaultClient.Do(req)
		requireNoError(err)
		defer safeClose(resp.Body)
		
		// Accept OK (200), NotFound (404), or Conflict (409)
		// 409 means schema is still associated with a table, which is acceptable
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
			return
		}
		
		if resp.StatusCode == http.StatusConflict {
			// Schema still associated with table, wait and retry
			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
			// On last attempt, accept 409 as it's not critical
			return
		}
		
		// Unexpected status code
		dump, _ := httputil.DumpResponse(resp, true)
		panic(fmt.Sprintf("Unexpected status code: %d %s", resp.StatusCode, string(dump)))
	}
}

func waitForTableSchema(schemaName string, timeout time.Duration) {
	pollTicker := time.NewTicker(PollInterval)
	defer pollTicker.Stop()

	timeoutTicker := time.NewTimer(timeout)
	defer timeoutTicker.Stop()

	isReady := func() bool {
		req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/schemas/"+schemaName, nil)
		requireNoError(err)
		resp, err := http.DefaultClient.Do(req)
		requireNoError(err)
		defer safeClose(resp.Body)
		return resp.StatusCode == http.StatusOK
	}

	if isReady() {
		return
	}
	for {
		select {
		case <-timeoutTicker.C:
			panic(fmt.Sprintf("Timed out waiting for schema %s", schemaName))
		case <-pollTicker.C:
			if isReady() {
				return
			}
		}
	}
}

func tableExists(tableName string) bool {
	req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/tables/"+tableName+"/metadata", nil)
	requireNoError(err)

	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		panic(fmt.Sprintf("Unexpected status code: %d", resp.StatusCode))
	}
	return resp.StatusCode == http.StatusOK
}

func deleteTable(tableName string) {
	req, err := http.NewRequest(http.MethodDelete, ControllerUrl+"/tables/"+tableName, nil)
	requireNoError(err)

	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)
	requireStatus(resp, http.StatusOK, http.StatusNotFound)
}

func waitForTableDeletion(tableName string, timeout time.Duration) {
	pollTicker := time.NewTicker(PollInterval)
	defer pollTicker.Stop()

	timeoutTicker := time.NewTimer(timeout)
	defer timeoutTicker.Stop()

	for {
		if !tableExists(tableName) {
			// Table is gone, but give Pinot extra time to clean up all metadata:
			// - External view cleanup
			// - Table config cleanup
			// - Schema associations
			fmt.Printf("Table %s deleted, waiting for full cleanup...\n", tableName)
			time.Sleep(5 * time.Second)
			return
		}

		select {
		case <-timeoutTicker.C:
			panic(fmt.Sprintf("Timed out waiting for table %s to be deleted", tableName))
		case <-pollTicker.C:
		}
	}
}

func createTableConfig(configFile string) {
	create := func() (int, string) {
		file, err := TestDataFS.Open(configFile)
		requireNoError(err)
		defer safeClose(file)

		req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/tables", file)
		requireNoError(err)

		resp, err := http.DefaultClient.Do(req)
		requireNoError(err)
		defer safeClose(resp.Body)

		var respBody bytes.Buffer
		_, err = respBody.ReadFrom(resp.Body)
		requireNoError(err)

		return resp.StatusCode, respBody.String()
	}

	var code int
	var body string
	maxRetries := 5
	retryDelay := 2 * time.Second
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		code, body = create()
		if code == http.StatusOK {
			return
		}
		
		// If table config already exists (409), wait longer for cleanup
		if code == http.StatusConflict && attempt < maxRetries-1 {
			fmt.Printf("Table config already exists (attempt %d/%d), waiting for cleanup...\n", attempt+1, maxRetries)
			time.Sleep(retryDelay * time.Duration(attempt+1)) // Exponential backoff
			continue
		}
		
		// For other errors, wait a bit and retry
		if attempt < maxRetries-1 {
			time.Sleep(retryDelay)
			continue
		}
	}
	
	if code != http.StatusOK {
		panic(fmt.Sprintf("Unexpected status code: %d %s", code, body))
	}
}

func waitForTableReady(tableName string, timeout time.Duration) {
	pollTicker := time.NewTicker(PollInterval)
	defer pollTicker.Stop()

	timeoutTicker := time.NewTimer(timeout)
	defer timeoutTicker.Stop()

	isReady := func() bool {
		// Check if table exists and is accessible
		req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/tables/"+tableName, nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer safeClose(resp.Body)
		
		// Table should return 200 when fully initialized
		return resp.StatusCode == http.StatusOK
	}

	// Give it an initial moment to start initializing
	time.Sleep(2 * time.Second)
	
	if isReady() {
		// Extra wait to ensure table is fully ready to accept data
		time.Sleep(1 * time.Second)
		return
	}
	
	for {
		select {
		case <-timeoutTicker.C:
			panic(fmt.Sprintf("Timed out waiting for table %s to be ready", tableName))
		case <-pollTicker.C:
			if isReady() {
				// Extra wait to ensure table is fully ready to accept data
				time.Sleep(1 * time.Second)
				return
			}
		}
	}
}

func uploadJsonTableData(tableNameWithType string, dataFile string) {
	var body bytes.Buffer
	multipartWriter := multipart.NewWriter(&body)
	defer safeClose(multipartWriter)

	formWriter, err := multipartWriter.CreateFormFile("file", dataFile)
	requireNoError(err)

	file, err := TestDataFS.Open(dataFile)
	requireNoError(err)
	defer safeClose(file)

	_, err = io.Copy(formWriter, file)
	requireNoError(err)

	batchConfigMapJson, err := json.Marshal(map[string]string{
		"inputFormat": strings.TrimPrefix(filepath.Ext(dataFile), "."),
	})
	requireNoError(err)

	values := make(url.Values)
	values.Add("tableNameWithType", tableNameWithType)
	values.Add("batchConfigMapStr", string(batchConfigMapJson))

	safeClose(multipartWriter)
	req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/ingestFromFile?"+values.Encode(), &body)
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)

	requireNoError(err)
	requireOkStatus(resp)
}

func requireOkStatus(resp *http.Response) {
	requireStatus(resp, http.StatusOK)
}

func requireStatus(resp *http.Response, codes ...int) {
	if !slices.Contains(codes, resp.StatusCode) {
		dump, _ := httputil.DumpResponse(resp, true)
		panic(fmt.Sprintf("Unexpected status code: %d %s", resp.StatusCode, string(dump)))
	}
}

func safeClose(closer io.Closer) {
	requireNoError(closer.Close())
}
