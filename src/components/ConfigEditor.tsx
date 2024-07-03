import React, { ChangeEvent, useEffect, useState } from 'react';
import { InlineField, Input, PopoverContent, SecretInput, Select } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { PinotConnectionConfig, PinotSecureConfig } from '../types/PinotConnectionConfig';
import { DataSourceDescription } from '@grafana/experimental';

interface ConfigEditorProps extends DataSourcePluginOptionsEditorProps<PinotConnectionConfig> {}

export function ConfigEditor(props: ConfigEditorProps) {
  const { onOptionsChange, options } = props;

  const onConfigChange = (config: PinotConnectionConfig) => onOptionsChange({ ...options, jsonData: config });
  const onSecureConfigChange = (secureConfig: PinotSecureConfig) =>
    onOptionsChange({
      ...options,
      secureJsonData: secureConfig,
    });

  const { jsonData, secureJsonFields } = options;
  const secureJsonData = (options.secureJsonData || {}) as PinotSecureConfig;

  return (
    <>
      <DataSourceDescription dataSourceName="Pinot" docsLink="#" />
      <hr style={{ marginTop: '50px', marginBottom: '56px' }} />
      <h3>Connection</h3>
      <div className="gf-form-group">
        <InputUrl
          label={'Controller URL'}
          tooltip={
            <>
              Specify a complete HTTP URL
              <br />
              (for example https://example.com:8080)
            </>
          }
          placeholder={'Controller URL'}
          value={jsonData.controllerUrl}
          onChange={(controllerUrl) => onConfigChange({ ...jsonData, controllerUrl })}
        />
        <InputUrl
          label={'Broker URL'}
          tooltip={
            <>
              Specify a complete HTTP URL
              <br />
              (for example https://example.com:8080)
            </>
          }
          placeholder={'Broker URL'}
          value={jsonData.brokerUrl}
          onChange={(brokerUrl) => onConfigChange({ ...jsonData, brokerUrl })}
        />
      </div>
      <h3>Authentication</h3>
      <p>
        This plugin requires a Pinot authentication token. For detailed instructions on generating a token,{' '}
        <a href={'https://dev.startree.ai/docs/query-data/use-apis-and-build-apps/generate-an-api-token'}>
          view the documentation
        </a>
        .
      </p>
      <div className="gf-form-group">
        <InputPinotToken
          isConfigured={!!secureJsonFields?.apiKey}
          tokenType={jsonData.tokenType}
          defaultTokenType={'Basic'}
          tokenTypeOptions={['Basic', 'Bearer']}
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

function SelectConfigEditorDatabase(props: {
  selected: string | undefined;
  tokenType: string | undefined;
  tokenValue: string | undefined;
  onChange: (val: string) => void;
}) {
  const { selected, tokenType, tokenValue, onChange } = props;
  const defaultValue = 'default';

  const options = useConfigEditorDatabases(selected, tokenType, tokenValue);

  if (options?.length == 0 && selected == undefined) {
    onChange(defaultValue);
  } else if (options?.length == 1 && selected == undefined) {
    onChange(options[0]);
  }

  if (options?.length == 0) {
    options.push(defaultValue);
  }

  return (
    <div className={'gf-form'}>
      <InlineField
        label={'Database'}
        labelWidth={24}
        tooltip={
          <p className={'text-secondary'}>
            Optionally select Pinot database for this connection. Leave this field blank to choose the database in the
            panel editor.
          </p>
        }
      >
        <Select
          width={40}
          onChange={(event) => onChange(event.currentTarget.value)}
          value={selected}
          options={options?.map((name) => ({ label: name, value: name }))}
        />
      </InlineField>
    </div>
  );
}

function useConfigEditorDatabases(
  controllerUrl: string | undefined,
  tokenType: string | undefined,
  tokenValue: string | undefined
): string[] | undefined {
  const [data, setData] = useState<string[] | undefined>(undefined);

  useEffect(() => {
    if (!controllerUrl || !tokenType || !tokenValue) {
      // Nothing to do.
      return;
    }

    fetch(controllerUrl, {
      method: 'GET',
      headers: { Authorization: `${tokenType} ${tokenValue}`, Accept: 'application/json' },
    })
      .then((resp): Promise<string[]> => {
        switch (resp.status) {
          case 200:
            return resp.json();
          case 404:
            return new Promise((): string[] => []);
          default:
            throw new Error('Invalid Pinot connection settings');
        }
      })
      .then((respData: string[]) => setData(respData));
  }, [controllerUrl, tokenType, tokenValue]);

  return data;
}

function InputUrl(props: {
  label: string;
  tooltip: PopoverContent;
  placeholder: string;
  value: string | undefined;
  onChange: (val: string) => void;
}) {
  const isValidUrl = (url: string | undefined) => {
    return url ? /^(http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/.test(url) : true;
  };

  const { label, value, tooltip, placeholder, onChange } = props;
  const isValid = isValidUrl(value);

  return (
    <InlineField
      label={label}
      labelWidth={24}
      tooltip={tooltip}
      grow
      required
      invalid={!isValid}
      error={isValid ? '' : 'Please enter a valid URL'}
      interactive
    >
      <Input
        width={40}
        onChange={(event) => onChange(event.currentTarget.value)}
        value={value}
        placeholder={placeholder}
      />
    </InlineField>
  );
}

function InputPinotToken(props: {
  isConfigured: boolean;
  tokenType: string | undefined;
  defaultTokenType: string;
  tokenTypeOptions: string[];
  tokenValue: string | undefined;
  onChangeType: (val: string | undefined) => void;
  onChangeToken: (val: string | undefined) => void;
  onResetToken: () => void;
}) {
  const {
    isConfigured,
    tokenType,
    tokenTypeOptions,
    tokenValue,
    defaultTokenType,
    onChangeToken,
    onChangeType,
    onResetToken,
  } = props;

  return (
    <div className={'gf-form'}>
      <InlineField label={'Type'} labelWidth={8} required>
        <Select
          options={tokenTypeOptions.map((v) => ({ label: v, value: v }))}
          isSearchable={false}
          value={tokenType || defaultTokenType}
          width={12}
          onChange={(change) => onChangeType(change.value)}
        />
      </InlineField>
      <InlineField label={'Token'} labelWidth={8} required>
        <SecretInput
          isConfigured={isConfigured}
          value={tokenValue}
          placeholder="Token"
          width={40}
          onReset={onResetToken}
          onChange={(event: ChangeEvent<HTMLInputElement>) => onChangeToken(event.target.value)}
        />
      </InlineField>
    </div>
  );
}
