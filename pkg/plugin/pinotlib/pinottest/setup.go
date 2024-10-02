package pinottest

import (
	"bytes"
	"embed"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

import _ "embed"

//go:embed data/*
var dataFS embed.FS

const (
	ControllerUrl = "http://localhost:9000"
	BrokerUrl     = "http://localhost:8000"

	InfraMetricsTableName    = "infraMetrics"
	GithubEventsTableName    = "githubEvents"
	StarbucksStoresTableName = "starbucksStores"
	AirlineStatsTableName    = "airlineStats"
)

var createTestTablesOnce sync.Once

func CreateTestTables(t *testing.T) {
	createTestTablesOnce.Do(func() {
		WaitForPinot(t, 5*time.Minute)

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
		}

		var wg sync.WaitGroup
		wg.Add(len(jobs))

		setupTable := func(job CreateTableJob) {
			defer wg.Done()
			if !schemaExists(t, job.tableName) {
				t.Logf("Creating schema for %s table schema...", job.tableName)
				createTableSchema(t, job.schemaFile)
			}
			if !tableExists(t, job.tableName) {
				t.Logf("Creating %s table config...", job.tableName)
				createTableConfig(t, job.configFile)
			}
			if !(tableHasData(t, job.tableName) || job.dataFile == "") {
				t.Logf("Uploading %s table data...", job.tableName)
				uploadJsonTableData(t, job.tableName+"_OFFLINE", job.dataFile)
			}
		}

		for _, job := range jobs {
			go setupTable(job)
		}
		wg.Wait()
		t.Log("Pinot setup complete.")
	})
}

func WaitForPinot(t *testing.T, timeout time.Duration) {
	pollTicker := time.NewTicker(time.Second)
	defer pollTicker.Stop()

	timeoutTicker := time.NewTimer(timeout)
	defer timeoutTicker.Stop()

	isReady := func() bool {
		req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/instances", nil)
		req.Header.Set("Accept", "application/json")
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer safeClose(t, resp.Body)

		if resp.StatusCode != http.StatusOK {
			return false
		}
		var respData struct {
			Instances []string `json:"instances"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respData))

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
	t.Log("Waiting for Pinot...")
	for {
		select {
		case <-timeoutTicker.C:
			t.Fatal("Timed out waiting for Pinot")
		case <-pollTicker.C:
			if isReady() {
				return
			}
		}
	}
}

func schemaExists(t *testing.T, schemaName string) bool {
	req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/schemas/"+schemaName, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer safeClose(t, resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		t.Errorf("Unexpected status code: %d", resp.StatusCode)
	}
	return resp.StatusCode == http.StatusOK
}

func createTableSchema(t *testing.T, schemaFile string) {
	var body bytes.Buffer
	multipartWriter := multipart.NewWriter(&body)
	defer safeClose(t, multipartWriter)

	formWriter, err := multipartWriter.CreateFormFile("schemaName", schemaFile)
	require.NoError(t, err)

	file, err := dataFS.Open(schemaFile)
	require.NoError(t, err)
	defer safeClose(t, file)

	_, err = io.Copy(formWriter, file)
	require.NoError(t, err)

	safeClose(t, multipartWriter)
	req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/schemas", &body)
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer safeClose(t, resp.Body)

	var respBody bytes.Buffer
	_, err = respBody.ReadFrom(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, respBody.String())
}

func tableExists(t *testing.T, tableName string) bool {
	req, err := http.NewRequest(http.MethodGet, ControllerUrl+"/tables/"+tableName+"/metadata", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer safeClose(t, resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		t.Errorf("Unexpected status code: %d", resp.StatusCode)
	}
	return resp.StatusCode == http.StatusOK
}

func createTableConfig(t *testing.T, configFile string) {
	file, err := dataFS.Open(configFile)
	require.NoError(t, err)
	defer safeClose(t, file)

	req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/tables", file)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer safeClose(t, resp.Body)

	var respBody bytes.Buffer
	_, err = respBody.ReadFrom(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, respBody.String())
}

func tableHasData(t *testing.T, tableName string) bool {
	reqData := struct {
		Sql string `json:"sql"`
	}{
		Sql: fmt.Sprintf("select * from %s limit 1", tableName),
	}

	var reqBody bytes.Buffer
	require.NoError(t, json.NewEncoder(&reqBody).Encode(reqData))

	req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/sql", &reqBody)
	req.Header.Set("Content-Type", "application/json")
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer safeClose(t, resp.Body)

	var respData struct {
		ResultTable struct {
			Rows []interface{} `json:"rows"`
		} `json:"resultTable"`
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&respData))

	return len(respData.ResultTable.Rows) != 0
}

func uploadJsonTableData(t *testing.T, tableNameWithType string, dataFile string) {
	var body bytes.Buffer
	multipartWriter := multipart.NewWriter(&body)
	defer safeClose(t, multipartWriter)

	formWriter, err := multipartWriter.CreateFormFile("file", dataFile)
	require.NoError(t, err)

	file, err := dataFS.Open(dataFile)
	require.NoError(t, err)
	defer safeClose(t, file)

	_, err = io.Copy(formWriter, file)
	require.NoError(t, err)

	batchConfigMapJson, err := json.Marshal(map[string]string{
		"inputFormat": strings.TrimPrefix(filepath.Ext(dataFile), "."),
	})
	require.NoError(t, err)

	values := make(url.Values)
	values.Add("tableNameWithType", tableNameWithType)
	values.Add("batchConfigMapStr", string(batchConfigMapJson))

	safeClose(t, multipartWriter)
	req, err := http.NewRequest(http.MethodPost, ControllerUrl+"/ingestFromFile?"+values.Encode(), &body)
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer safeClose(t, resp.Body)

	var respBody bytes.Buffer
	_, err = respBody.ReadFrom(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, respBody.String())
}

func safeClose(t *testing.T, closer io.Closer) {
	require.NoError(t, closer.Close())
}
