package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

const TokenTypeNone = "None"

type PinotDataSourceConfig struct {
	ControllerUrl string
	BrokerUrl     string
	DatabaseName  string

	// Secrets
	Authorization string
}

func PinotDataSourceConfigFrom(settings backend.DataSourceInstanceSettings) (*PinotDataSourceConfig, error) {
	var config struct {
		ControllerUrl string `json:"controllerUrl"`
		BrokerUrl     string `json:"brokerUrl"`
		DatabaseName  string `json:"databaseName"`
		TokenType     string `json:"tokenType"`
	}

	tokenSecret := settings.DecryptedSecureJSONData["authToken"]
	if err := json.Unmarshal(settings.JSONData, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal datasource config: %w", err)
	} else if config.BrokerUrl == "" {
		return nil, errors.New("broker url cannot be empty")
	} else if config.ControllerUrl == "" {
		return nil, errors.New("controller url cannot be empty")
	} else if config.TokenType == "" {
		return nil, errors.New("token type cannot be empty")
	}

	var authToken string
	if config.TokenType != TokenTypeNone {
		authToken = fmt.Sprintf("%s %s", config.TokenType, tokenSecret)
	}

	return &PinotDataSourceConfig{
		ControllerUrl: config.ControllerUrl,
		BrokerUrl:     config.BrokerUrl,
		DatabaseName:  config.DatabaseName,
		Authorization: authToken,
	}, nil
}
