import React, { useState } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { PinotConnectionConfig, PinotSecureConfig } from '../../config/PinotConnectionConfig';
import { DataSourceDescription } from '@grafana/experimental';
import { InputPinotToken } from './InputPinotToken';
import { InputUrl } from './InputUrl';
import allLabels from 'labels';
import { InlineField, InlineSwitch, Input, useTheme2 } from '@grafana/ui';
import { css } from '@emotion/css';
import { InputDatabase } from './InputDatabase';
import { SelectQueryOptions } from './SelectQueryOptions';
import { InputTlsSettings } from './InputTlsSettings';
import { InputCustomHeaders } from './InputCustomHeaders';

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
      <h3>Query</h3>
      <div className="gf-form-group">
        <InlineField
          data-testid="input-query-timeout"
          label={labels.queryTimeout.label}
          labelWidth={24}
          tooltip={labels.queryTimeout.tooltip}
          grow
          interactive
        >
          <Input
            type="number"
            width={40}
            min={0}
            placeholder={labels.queryTimeout.placeholder}
            value={jsonData.queryTimeoutSeconds ?? ''}
            onChange={(event) => {
              const value = event.currentTarget.value;
              onConfigChange({ ...jsonData, queryTimeoutSeconds: value === '' ? undefined : Number(value) });
            }}
          />
        </InlineField>
        <InlineField
          data-testid="input-max-row-limit"
          label={labels.maxRowLimit.label}
          labelWidth={24}
          tooltip={labels.maxRowLimit.tooltip}
          grow
          interactive
        >
          <Input
            type="number"
            width={40}
            min={0}
            placeholder={labels.maxRowLimit.placeholder}
            value={jsonData.maxRowLimit ?? ''}
            onChange={(event) => {
              const value = event.currentTarget.value;
              onConfigChange({ ...jsonData, maxRowLimit: value === '' ? undefined : Number(value) });
            }}
          />
        </InlineField>
      </div>
      <h3>Authentication</h3>
      <p className={styles.text}>
        If Grafana uses OAuth for user logins, this option directs Grafana to authenticate with Pinot using the user
        token instead of an API token.
      </p>
      <div className="gf-form-group">
        <InlineField
          data-testid="switch-oauth-pass-thru"
          label={'Enable OAuth Pass-Through'}
          labelWidth={24}
          tooltip={''}
          grow
          interactive
        >
          <InlineSwitch
            value={jsonData.oauthPassThru || false}
            onChange={() => onConfigChange({ ...jsonData, oauthPassThru: !jsonData.oauthPassThru })}
          />
        </InlineField>
      </div>
      <p className={styles.text} data-testid="auth-description">
        Configure a Pinot API token. For detailed instructions on generating a token,{' '}
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

      <h3 data-testid="tls-heading">TLS / SSL Settings</h3>
      <div className="gf-form-group">
        <InputTlsSettings options={options} onOptionsChange={onOptionsChange} />
      </div>

      <h3 data-testid="custom-headers-heading">Custom HTTP Headers</h3>
      <div className="gf-form-group">
        <InputCustomHeaders options={options} onOptionsChange={onOptionsChange} />
      </div>

      <h3 data-testid="traces-to-logs-heading">Trace to logs</h3>
      <p className={styles.text}>{labels.tracesToLogs.description}</p>
      <div className="gf-form-group">
        <InlineField
          data-testid="input-traces-to-logs-table"
          label={labels.tracesToLogs.table.label}
          labelWidth={24}
          tooltip={labels.tracesToLogs.table.tooltip}
          grow
          interactive
        >
          <Input
            width={40}
            placeholder={labels.tracesToLogs.table.placeholder}
            value={jsonData.tracesToLogsTable ?? ''}
            onChange={(event) =>
              onConfigChange({ ...jsonData, tracesToLogsTable: event.currentTarget.value || undefined })
            }
          />
        </InlineField>
        <InlineField
          data-testid="input-traces-to-logs-trace-id-column"
          label={labels.tracesToLogs.traceIdColumn.label}
          labelWidth={24}
          tooltip={labels.tracesToLogs.traceIdColumn.tooltip}
          grow
          interactive
        >
          <Input
            width={40}
            placeholder={labels.tracesToLogs.traceIdColumn.placeholder}
            value={jsonData.tracesToLogsTraceIdColumn ?? ''}
            onChange={(event) =>
              onConfigChange({ ...jsonData, tracesToLogsTraceIdColumn: event.currentTarget.value || undefined })
            }
          />
        </InlineField>
        <InlineField
          data-testid="input-traces-to-logs-time-column"
          label={labels.tracesToLogs.timeColumn.label}
          labelWidth={24}
          tooltip={labels.tracesToLogs.timeColumn.tooltip}
          grow
          interactive
        >
          <Input
            width={40}
            placeholder={labels.tracesToLogs.timeColumn.placeholder}
            value={jsonData.tracesToLogsTimeColumn ?? ''}
            onChange={(event) =>
              onConfigChange({ ...jsonData, tracesToLogsTimeColumn: event.currentTarget.value || undefined })
            }
          />
        </InlineField>
        <InlineField
          data-testid="input-traces-to-logs-log-column"
          label={labels.tracesToLogs.logColumn.label}
          labelWidth={24}
          tooltip={labels.tracesToLogs.logColumn.tooltip}
          grow
          interactive
        >
          <Input
            width={40}
            placeholder={labels.tracesToLogs.logColumn.placeholder}
            value={jsonData.tracesToLogsLogColumn ?? ''}
            onChange={(event) =>
              onConfigChange({ ...jsonData, tracesToLogsLogColumn: event.currentTarget.value || undefined })
            }
          />
        </InlineField>
      </div>

      <h3 data-testid="logs-to-traces-heading">Logs to trace</h3>
      <p className={styles.text}>{labels.logsToTraces.description}</p>
      <div className="gf-form-group">
        <InlineField
          data-testid="input-logs-to-traces-table"
          label={labels.logsToTraces.table.label}
          labelWidth={24}
          tooltip={labels.logsToTraces.table.tooltip}
          grow
          interactive
        >
          <Input
            width={40}
            placeholder={labels.logsToTraces.table.placeholder}
            value={jsonData.logsToTracesTable ?? ''}
            onChange={(event) =>
              onConfigChange({ ...jsonData, logsToTracesTable: event.currentTarget.value || undefined })
            }
          />
        </InlineField>
        <InlineField
          data-testid="input-logs-to-traces-trace-id-column"
          label={labels.logsToTraces.traceIdColumn.label}
          labelWidth={24}
          tooltip={labels.logsToTraces.traceIdColumn.tooltip}
          grow
          interactive
        >
          <Input
            width={40}
            placeholder={labels.logsToTraces.traceIdColumn.placeholder}
            value={jsonData.logsToTracesTraceIdColumn ?? ''}
            onChange={(event) =>
              onConfigChange({ ...jsonData, logsToTracesTraceIdColumn: event.currentTarget.value || undefined })
            }
          />
        </InlineField>
        <InlineField
          data-testid="input-logs-to-traces-time-column"
          label={labels.logsToTraces.timeColumn.label}
          labelWidth={24}
          tooltip={labels.logsToTraces.timeColumn.tooltip}
          grow
          interactive
        >
          <Input
            width={40}
            placeholder={labels.logsToTraces.timeColumn.placeholder}
            value={jsonData.logsToTracesTimeColumn ?? ''}
            onChange={(event) =>
              onConfigChange({ ...jsonData, logsToTracesTimeColumn: event.currentTarget.value || undefined })
            }
          />
        </InlineField>
      </div>
    </>
  );
}
