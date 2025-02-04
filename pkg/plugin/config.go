package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"net/http"
)

const TokenTypeNone = "None"

type Config struct {
	ControllerUrl string        `json:"controllerUrl"`
	BrokerUrl     string        `json:"brokerUrl"`
	DatabaseName  string        `json:"databaseName"`
	TokenType     string        `json:"tokenType"`
	QueryOptions  []QueryOption `json:"queryOptions"`

	// Secrets
	TokenSecret string `json:"-"`
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
	} else if config.TokenType == "" {
		return errors.New("token type cannot be empty")
	}

	config.TokenSecret = settings.DecryptedSecureJSONData["authToken"]
	return nil
}

func PinotClientOf(httpClient *http.Client, config Config) *pinotlib.PinotClient {
	var authorization string
	if config.TokenType != TokenTypeNone {
		authorization = fmt.Sprintf("%s %s", config.TokenType, config.TokenSecret)
	}

	var queryOptions []pinotlib.QueryOption
	for _, o := range config.QueryOptions {
		if o.Name != "" || o.Value != "" {
			queryOptions = append(queryOptions, pinotlib.QueryOption{Name: o.Name, Value: o.Value})
		}
	}

	return pinotlib.NewPinotClient(httpClient, pinotlib.PinotClientProperties{
		ControllerUrl: config.ControllerUrl,
		BrokerUrl:     config.BrokerUrl,
		DatabaseName:  config.DatabaseName,
		QueryOptions:  queryOptions,
		Authorization: authorization,
	})
}
