import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput, Select } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps, SelectableValue } from '@grafana/data';
import { MyDataSourceOptions, MySecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<MyDataSourceOptions> {}

const connectionTypeOptions = [
  { label: 'Zookeeper', value: 0 },
  { label: 'Controller', value: 1 },
  { label: 'Broker', value: 2 }
];

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;

  const onTypeChange = (value: SelectableValue<number>) => {
    console.log(value.label)
    const jsonData = {
      ...options.jsonData,
      type: value.label,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onPathChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      url: event.target.value,
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
  const secureJsonData = (options.secureJsonData || {}) as MySecureJsonData;

  return (
    <div className="gf-form-group">
      <InlineField label="connectionType" labelWidth={24}>
        <Select
            options={connectionTypeOptions}
            value={connectionTypeOptions.find(option => option.label === jsonData.type) || connectionTypeOptions[1]}
            onChange={onTypeChange}
        />
      </InlineField>

      <InlineField label="url" labelWidth={24}>
        <Input
          onChange={onPathChange}
          value={jsonData.url || ''}
          placeholder="json field returned to frontend"
          width={40}
        />
      </InlineField>

      <InlineField label="Auth Key" labelWidth={24}>
        <SecretInput
          isConfigured={(secureJsonFields && secureJsonFields.apiKey) as boolean}
          value={secureJsonData.authToken || ''}
          placeholder="secure json field (backend only)"
          width={40}
          onReset={onResetAPIKey}
          onChange={onAPIKeyChange}
        />
      </InlineField>
    </div>
  );
}
