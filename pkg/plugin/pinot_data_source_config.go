package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

type PinotDataSourceConfig struct {
	ControllerUrl string
	BrokerUrl     string
	TokenType     string

	// Secrets
	AuthToken string
}

func PinotDataSourceConfigFrom(settings backend.DataSourceInstanceSettings) (*PinotDataSourceConfig, error) {
	var config struct {
		ControllerUrl string `json:"controllerUrl"`
		BrokerUrl     string `json:"brokerUrl"`
		TokenType     string `json:"tokenType"`
	}
	authToken := settings.DecryptedSecureJSONData["authToken"]

	if err := json.Unmarshal(settings.JSONData, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal datasource config: %w", err)
	} else if config.BrokerUrl == "" {
		return nil, errors.New("broker url cannot be empty")
	} else if config.ControllerUrl == "" {
		return nil, errors.New("controller url cannot be empty")
	} else if config.TokenType == "" {
		return nil, errors.New("token type cannot be empty")
	} else if authToken == "" {
		return nil, errors.New("auth token cannot be empty")
	}

	return &PinotDataSourceConfig{
		ControllerUrl: config.ControllerUrl,
		BrokerUrl:     config.BrokerUrl,
		TokenType:     config.TokenType,
		AuthToken:     authToken,
	}, nil
}
