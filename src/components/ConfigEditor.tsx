import React, { useState } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { PinotConnectionConfig, PinotSecureConfig } from '../types/PinotConnectionConfig';
import { DataSourceDescription } from '@grafana/experimental';
import { InputPinotToken } from './InputPinotToken';
import { InputUrl } from './InputUrl';
import allLabels from 'labels';

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

  // Auto-populate broker url based on controller url.
  const [formBrokerUrl, setFormBrokerUrl] = useState<string | undefined>(undefined);

  return (
    <>
      <DataSourceDescription dataSourceName={labels.dataSourceName} docsLink={labels.docsLinks} />
      <hr style={{ marginTop: '50px', marginBottom: '56px' }} />
      <h3>Connection</h3>
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
      </div>
      <h3>Authentication</h3>
      <p>
        This plugin requires a Pinot authentication token. For detailed instructions on generating a token,{' '}
        <a href={labels.token.help}>view the documentation</a>.
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
