package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"net/http"
	"time"
)

const (
	TokenTypeNone              = "None"
	DefaultQueryTimeoutSeconds = 60
)

type Config struct {
	ControllerUrl string        `json:"controllerUrl"`
	BrokerUrl     string        `json:"brokerUrl"`
	DatabaseName  string        `json:"databaseName"`
	TokenType     string        `json:"tokenType"`
	QueryOptions  []QueryOption `json:"queryOptions"`
	OAuthPassThru bool          `json:"oauthPassThru"`

	// QueryTimeoutSeconds bounds a single broker query. Empty/zero falls back to the default.
	QueryTimeoutSeconds int `json:"queryTimeoutSeconds"`
	// MaxRowLimit caps result size for queries without an explicit LIMIT. Zero disables it.
	MaxRowLimit int `json:"maxRowLimit"`

	// Transport security (non-secret). These mirror the standard keys the
	// Grafana SDK reads. The actual *http.Client (TLS, headers, proxy) is built
	// from settings.HTTPClientOptions in NewInstance, so these fields are parsed
	// only for validation/visibility, not consumed by PinotClientOf.
	// ponytail: enforcement lives in HTTPClientOptions; don't duplicate it here.
	TLSSkipVerify     bool   `json:"tlsSkipVerify"`
	TLSAuth           bool   `json:"tlsAuth"`
	TLSAuthWithCACert bool   `json:"tlsAuthWithCACert"`
	TLSServerName     string `json:"serverName"`

	// Trace-to-logs correlation: when fully set, the traces builder attaches a data link from each
	// span to a logs query against this table, keyed by the span's trace id.
	TracesToLogsTable         string `json:"tracesToLogsTable"`
	TracesToLogsTraceIdColumn string `json:"tracesToLogsTraceIdColumn"`
	TracesToLogsTimeColumn    string `json:"tracesToLogsTimeColumn"`
	TracesToLogsLogColumn     string `json:"tracesToLogsLogColumn"`

	// Secrets
	TokenSecret string `json:"-"`
}

// TracesToLogsConfig projects the datasource's trace-to-logs settings into the dataquery package's
// mapping type used when building span data links.
func (config *Config) TracesToLogsConfig() dataquery.TracesToLogsConfig {
	return dataquery.TracesToLogsConfig{
		Table:         config.TracesToLogsTable,
		TraceIdColumn: config.TracesToLogsTraceIdColumn,
		TimeColumn:    config.TracesToLogsTimeColumn,
		LogColumn:     config.TracesToLogsLogColumn,
	}
}

// QueryTimeout returns the configured broker query timeout, defaulting when unset.
func (config *Config) QueryTimeout() time.Duration {
	if config.QueryTimeoutSeconds <= 0 {
		return DefaultQueryTimeoutSeconds * time.Second
	}
	return time.Duration(config.QueryTimeoutSeconds) * time.Second
}

type QueryOption struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (config *Config) ReadFrom(settings backend.DataSourceInstanceSettings) error {
	if err := json.Unmarshal(settings.JSONData, &config); err != nil {
		return fmt.Errorf("failed to unmarshal datasource config: %w", err)
	} else if config.BrokerUrl == "" {
		return errors.New("broker url cannot be empty")
	} else if config.ControllerUrl == "" {
		return errors.New("controller url cannot be empty")
	}

	config.TokenSecret = settings.DecryptedSecureJSONData["authToken"]
	return nil
}

func PinotClientOf(httpClient *http.Client, config Config) *pinot.Client {
	var authorization string
	if !config.OAuthPassThru && config.TokenType != TokenTypeNone {
		authorization = fmt.Sprintf("%s %s", config.TokenType, config.TokenSecret)
	}

	var queryOptions []pinot.QueryOption
	for _, o := range config.QueryOptions {
		if o.Name != "" || o.Value != "" {
			queryOptions = append(queryOptions, pinot.QueryOption{Name: o.Name, Value: o.Value})
		}
	}

	return pinot.NewPinotClient(httpClient, pinot.ClientProperties{
		ControllerUrl: config.ControllerUrl,
		BrokerUrl:     config.BrokerUrl,
		DatabaseName:  config.DatabaseName,
		QueryOptions:  queryOptions,
		Authorization: authorization,
		QueryTimeout:  config.QueryTimeout(),
		MaxRowLimit:   config.MaxRowLimit,
	})
}
