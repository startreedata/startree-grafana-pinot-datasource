package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ backend.CallResourceHandler   = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

type DatasourceConfig struct {
	ControllerUrl string
	BrokerUrl     string
	DatabaseName  string

	// Secrets
	Authorization string
}

type Datasource struct {
	backend.CallResourceHandler
	backend.CheckHealthHandler
	backend.QueryDataHandler
	instancemgmt.InstanceDisposer
}

func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	config, err := DatasourceConfigFrom(settings)
	if err != nil {
		return nil, err
	}

	client, err := pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
		ControllerUrl: config.ControllerUrl,
		BrokerUrl:     config.BrokerUrl,
		DatabaseName:  config.DatabaseName,
		Authorization: config.Authorization,
	})
	if err != nil {
		return nil, err
	}

	return &Datasource{
		CallResourceHandler: NewCallResourceHandler(client),
		CheckHealthHandler:  NewCheckHealthHandler(client),
		QueryDataHandler:    NewQueryDataHandler(client),
		InstanceDisposer:    NewInstanceDisposer(),
	}, nil
}

const TokenTypeNone = "None"

func DatasourceConfigFrom(settings backend.DataSourceInstanceSettings) (*DatasourceConfig, error) {
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

	return &DatasourceConfig{
		ControllerUrl: config.ControllerUrl,
		BrokerUrl:     config.BrokerUrl,
		DatabaseName:  config.DatabaseName,
		Authorization: authToken,
	}, nil
}
