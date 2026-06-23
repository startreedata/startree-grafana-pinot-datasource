import React from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, InlineSwitch, Input, TLSAuthSettings } from '@grafana/ui';
import { PinotConnectionConfig } from '../../config/PinotConnectionConfig';

type Props = Pick<DataSourcePluginOptionsEditorProps<PinotConnectionConfig>, 'options' | 'onOptionsChange'>;

// Renders Grafana's standard TLS toggles + the reusable TLSAuthSettings cert
// fields. All values are written under the keys the SDK HTTP client expects
// (tlsSkipVerify, tlsAuth, tlsAuthWithCACert, serverName, secure tls*Cert/Key).
export function InputTlsSettings({ options, onOptionsChange }: Props) {
  const { jsonData } = options;
  const patchJsonData = (patch: Partial<PinotConnectionConfig>) =>
    onOptionsChange({ ...options, jsonData: { ...jsonData, ...patch } });

  const showServerName = jsonData.tlsAuth || jsonData.tlsAuthWithCACert;

  return (
    <>
      <InlineField
        label="Skip TLS Verify"
        labelWidth={24}
        interactive
        tooltip="Disable verification of the server's TLS certificate chain and host name. Not recommended outside of testing."
      >
        <InlineSwitch
          data-testid="switch-tls-skip-verify"
          value={jsonData.tlsSkipVerify || false}
          onChange={() => patchJsonData({ tlsSkipVerify: !jsonData.tlsSkipVerify })}
        />
      </InlineField>
      <InlineField
        label="TLS Client Auth"
        labelWidth={24}
        interactive
        tooltip="Authenticate to Pinot with a client certificate and key (mTLS)."
      >
        <InlineSwitch
          data-testid="switch-tls-client-auth"
          value={jsonData.tlsAuth || false}
          onChange={() => patchJsonData({ tlsAuth: !jsonData.tlsAuth })}
        />
      </InlineField>
      <InlineField
        label="With CA Cert"
        labelWidth={24}
        interactive
        tooltip="Verify the server's certificate with a custom CA certificate."
      >
        <InlineSwitch
          data-testid="switch-tls-ca-cert"
          value={jsonData.tlsAuthWithCACert || false}
          onChange={() => patchJsonData({ tlsAuthWithCACert: !jsonData.tlsAuthWithCACert })}
        />
      </InlineField>
      {showServerName && (
        <InlineField
          label="Server Name"
          labelWidth={24}
          interactive
          tooltip="Optional. Overrides the host name used to verify the server's TLS certificate (SNI)."
        >
          <Input
            data-testid="input-tls-server-name"
            width={40}
            placeholder="domain.example.com"
            value={jsonData.serverName || ''}
            onChange={(e) => patchJsonData({ serverName: e.currentTarget.value })}
          />
        </InlineField>
      )}
      <TLSAuthSettings dataSourceConfig={options} onChange={onOptionsChange} />
    </>
  );
}
