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
var dataFS embed.FS

const (
	Timeout      = 5 * time.Minute
	PollInterval = 1 * time.Second

	ControllerUrl = "http://localhost:9000"
	BrokerUrl     = "http://localhost:8000"

	InfraMetricsTableName       = "infraMetrics"
	GithubEventsTableName       = "githubEvents"
	StarbucksStoresTableName    = "starbucksStores"
	AirlineStatsTableName       = "airlineStats"
	BenchmarkTableName          = "benchmark"
	PartialTableName            = "partial"
	NginxLogsTableName          = "nginxLogs"
	DerivedTimeBucketsTableName = "derivedTimeBuckets"
)

var createTestTablesOnce sync.Once

func CreateTestTables() {
	createTestTablesOnce.Do(func() {
		WaitForPinot(Timeout)

		type CreateTableJob struct {
			tableName  string
			schemaFile string
			configFile string
			dataFile   string
		}

		jobs := []CreateTableJob{
			{
				tableName:  InfraMetricsTableName,
				schemaFile: "data/infraMetrics_schema.json",
				configFile: "data/infraMetrics_offline_table_config.json",
				dataFile:   "data/infraMetrics_data.json",
			},
			{
				tableName:  GithubEventsTableName,
				schemaFile: "data/githubEvents_schema.json",
				configFile: "data/githubEvents_offline_table_config.json",
				dataFile:   "data/githubEvents_data.json",
			},
			{
				tableName:  StarbucksStoresTableName,
				schemaFile: "data/starbucksStores_schema.json",
				configFile: "data/starbucksStores_offline_table_config.json",
				dataFile:   "data/starbucksStores_data.csv",
			},
			{
				tableName:  AirlineStatsTableName,
				schemaFile: "data/airlineStats_schema.json",
				configFile: "data/airlineStats_offline_table_config.json",
				// TODO: Add data file at some point
			},
			{
				tableName:  BenchmarkTableName,
				schemaFile: "data/benchmark_schema.json",
				configFile: "data/benchmark_offline_table_config.json",
				dataFile:   "data/benchmark_data.json",
			},
			{
				tableName:  PartialTableName,
				schemaFile: "data/partial_schema.json",
				configFile: "data/partial_offline_table_config.json",
				dataFile:   "data/partial_data_1.json",
			},
			{
				tableName:  NginxLogsTableName,
				schemaFile: "data/nginxLogs_schema.json",
				configFile: "data/nginxLogs_offline_table_config.json",
				dataFile:   "data/nginxLogs_data.json",
			},
			{
				tableName:  DerivedTimeBucketsTableName,
				schemaFile: "data/derivedTimeBuckets_schema.json",
				configFile: "data/derivedTimeBuckets_offline_table_config.json",
			},
		}

		var wg sync.WaitGroup
		wg.Add(len(jobs))

		var somethingChanged atomic.Bool
		setupTable := func(job CreateTableJob) {
			defer wg.Done()
			if tableExists(job.tableName) {
				return
			}

			fmt.Printf("Creating table %s...\n", job.tableName)
			somethingChanged.Store(true)
			deleteTableSchema(job.tableName)
			createTableSchema(job.schemaFile)
			waitForTableSchema(job.tableName, Timeout)
			createTableConfig(job.configFile)

			if job.dataFile == "" {
				return
			}
			uploadJsonTableData(job.tableName+"_OFFLINE", job.dataFile)
			waitForSegmentsAllGood(job.tableName, Timeout)

			// Delete the partial table's segment and upload a new segment
			if job.tableName == PartialTableName {
				uploadJsonTableData(PartialTableName+"_OFFLINE", "data/partial_data_2.json")
				waitForSegmentsAllGood(job.tableName, Timeout)
				segments := listOfflineSegments(job.tableName)
				if len(segments) != 2 {
					panic("expected 2 segments")
				}
				deleteSegmentFromFilesystem(segments[0])
				resetSegments(PartialTableName)
				waitForSegmentStatus(PartialTableName, segments[0], "BAD", Timeout)
			}
		}

		for _, job := range jobs {
			go setupTable(job)
		}
		wg.Wait()

		if somethingChanged.Load() {
			fmt.Println("Pinot setup complete.")
		}
	})
}

func waitForSegmentsAllGood(tableName string, timeout time.Duration) {
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
		if len(statuses) == goodSegments {
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

	file, err := dataFS.Open(schemaFile)
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
	req, err := http.NewRequest(http.MethodDelete, ControllerUrl+"/schemas/"+schemaName, nil)
	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)
	requireStatus(resp, http.StatusOK, http.StatusNotFound)
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

func createTableConfig(configFile string) {
	create := func() (int, string) {
		file, err := dataFS.Open(configFile)
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
	for i := 0; i < 3; i++ {
		code, body = create()
		if code == http.StatusOK {
			return
		}
	}
	if code != http.StatusOK {
		panic(fmt.Sprintf("Unexpected status code: %d %s", code, body))
	}
}

func tableHasData(tableName string) bool {
	reqData := struct {
		Sql string `json:"sql"`
	}{
		Sql: fmt.Sprintf("select * from %s limit 1", tableName),
	}

	var reqBody bytes.Buffer
	requireNoError(json.NewEncoder(&reqBody).Encode(reqData))

	req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/sql", &reqBody)
	req.Header.Set("Content-Type", "application/json")
	requireNoError(err)

	resp, err := http.DefaultClient.Do(req)
	requireNoError(err)
	defer safeClose(resp.Body)

	var respData struct {
		ResultTable struct {
			Rows []interface{} `json:"rows"`
		} `json:"resultTable"`
	}
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("Unexpected status code: %d %s", resp.StatusCode, reqBody.String()))
	}
	requireNoError(json.NewDecoder(resp.Body).Decode(&respData))

	return len(respData.ResultTable.Rows) != 0
}

func uploadJsonTableData(tableNameWithType string, dataFile string) {
	var body bytes.Buffer
	multipartWriter := multipart.NewWriter(&body)
	defer safeClose(multipartWriter)

	formWriter, err := multipartWriter.CreateFormFile("file", dataFile)
	requireNoError(err)

	file, err := dataFS.Open(dataFile)
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
