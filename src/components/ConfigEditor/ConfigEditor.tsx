import React, { useState } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { PinotConnectionConfig, PinotSecureConfig } from '../../dataquery/PinotConnectionConfig';
import { DataSourceDescription } from '@grafana/experimental';
import { InputPinotToken } from './InputPinotToken';
import { InputUrl } from './InputUrl';
import allLabels from 'labels';
import { Switch, useTheme2 } from '@grafana/ui';
import { css } from '@emotion/css';
import { InputDatabase } from './InputDatabase';
import { SelectQueryOptions } from './SelectQueryOptions';

interface ConfigEditorProps extends DataSourcePluginOptionsEditorProps<PinotConnectionConfig> {}

export function ConfigEditor(props: ConfigEditorProps) {
  const labels = allLabels.components.ConfigEditor;
  const { onOptionsChange, options } = props;

  const onConfigChange = (config: PinotConnectionConfig) => onOptionsChange({ ...options, jsonData: config });
  const onSecureConfigChange = (secureConfig: PinotSecureConfig) =>
    onOptionsChange({
      ...options,
      secureJsonData: secureConfig,
    });

  const { jsonData, secureJsonFields } = options;
  const secureJsonData = (options.secureJsonData || {}) as PinotSecureConfig;

  // Autopopulate broker url based on controller url.
  const [formBrokerUrl, setFormBrokerUrl] = useState<string | undefined>(undefined);

  const theme = useTheme2();

  // Copied styles from https://github.com/grafana/grafana-experimental/blob/2880c631232876bf6069619e096b4f2ca3457361/src/ConfigEditor/DataSourceDescription.tsx#L15
  const styles = {
    text: css({
      ...theme.typography.body,
      color: theme.colors.text.secondary,
      a: css({
        color: theme.colors.text.link,
        textDecoration: 'underline',
        '&:hover': {
          textDecoration: 'none',
        },
      }),
    }),
  };

  return (
    <>
      <DataSourceDescription dataSourceName={labels.dataSourceName} docsLink={labels.docsLinks} />
      <hr style={{ marginTop: '50px', marginBottom: '56px' }} />

      <p>Pass through oauth</p>
      <Switch
        value={jsonData.oauthPassThru}
        onChange={() => onConfigChange({ ...jsonData, oauthPassThru: !jsonData.oauthPassThru })}
      />
      <h3 data-testid="connection-heading">Connection</h3>
      <div className="gf-form-group">
        <InputUrl
          label={labels.controllerUrl.label}
          tooltip={
            <>
              Specify a complete HTTP URL
              <br />
              (for example https://example.com:8080)
            </>
          }
          placeholder={labels.controllerUrl.placeholder}
          value={jsonData.controllerUrl}
          onChange={(controllerUrl) => {
            let brokerUrl = jsonData.brokerUrl;
            if (!formBrokerUrl) {
              brokerUrl = controllerUrl.replace('pinot', 'broker.pinot');
            }

            onConfigChange({ ...jsonData, controllerUrl, brokerUrl });
          }}
        />
        <InputUrl
          data-testid="input-broker-url"
          label={labels.brokerUrl.label}
          tooltip={
            <>
              Specify a complete HTTP URL
              <br />
              (for example https://example.com:8080)
            </>
          }
          placeholder={labels.brokerUrl.placeholder}
          value={jsonData.brokerUrl}
          onChange={(brokerUrl) => {
            setFormBrokerUrl(brokerUrl);
            onConfigChange({ ...jsonData, brokerUrl });
          }}
        />
        <InputDatabase
          value={jsonData.databaseName}
          onChange={(databaseName) => onConfigChange({ ...jsonData, databaseName })}
        />
        <SelectQueryOptions
          selected={jsonData.queryOptions || []}
          onChange={(queryOptions) => onConfigChange({ ...jsonData, queryOptions })}
        />
      </div>
      <h3>Authentication</h3>
      <p className={styles.text} data-testid="auth-description">
        This plugin requires a Pinot authentication token. For detailed instructions on generating a token,{' '}
        <a href={labels.token.help} target="_blank" rel="noreferrer" data-testid="view-doc-link">
          view the documentation
        </a>
        .
      </p>
      <div className="gf-form-group">
        <InputPinotToken
          isConfigured={!!secureJsonFields?.apiKey}
          tokenType={jsonData.tokenType}
          tokenValue={secureJsonData.authToken}
          onChangeToken={(authToken) => onSecureConfigChange({ ...secureJsonData, authToken })}
          onChangeType={(tokenType) => onConfigChange({ ...jsonData, tokenType })}
          onResetToken={() =>
            onOptionsChange({
              ...options,
              secureJsonFields: {
                ...secureJsonFields,
                authToken: false,
              },
              secureJsonData: {
                ...secureJsonData,
                authToken: undefined,
              },
            })
          }
        />
      </div>
    </>
  );
}
