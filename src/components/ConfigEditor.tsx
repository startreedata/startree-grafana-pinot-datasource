import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { PinotConnectionConfig, PinotSecureConfig } from '../types/config';

interface Props extends DataSourcePluginOptionsEditorProps<PinotConnectionConfig> { }

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;

  const onControllerUrlChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      controllerUrl: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };


  const onBrokerUrlChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      brokerUrl: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  // Secure field (only sent to the backend)
  const onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        authToken: event.target.value,
      },
    });
  };

  const onResetAPIKey = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        authToken: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        authToken: '',
      },
    });
  };

  const { jsonData, secureJsonFields } = options;
  const secureJsonData = (options.secureJsonData || {}) as PinotSecureConfig;

  return (
    <div className="gf-form-group">

      <InlineField label="Pinot Controller Url" labelWidth={24}>
        <Input
          onChange={onControllerUrlChange}
          value={jsonData.controllerUrl || ''}
          placeholder="pinot controller url"
          width={40}
        />
      </InlineField>

      <InlineField label="Pinot Broker Url" labelWidth={24}>
        <Input
          onChange={onBrokerUrlChange}
          value={jsonData.brokerUrl || ''}
          placeholder="pinot broker url"
          width={40}
        />
      </InlineField>

      <InlineField label="Pinot API Token Key" labelWidth={24}>
        <SecretInput
          isConfigured={(secureJsonFields && secureJsonFields.apiKey) as boolean}
          value={secureJsonData.authToken || ''}
          placeholder="pinot api token"
          width={40}
          onReset={onResetAPIKey}
          onChange={onAPIKeyChange}
        />
      </InlineField>
    </div>
  );
}
