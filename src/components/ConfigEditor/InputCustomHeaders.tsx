import React from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { Button, IconButton, InlineField, Input, SecretInput } from '@grafana/ui';
import { PinotConnectionConfig, PinotSecureConfig } from '../../config/PinotConnectionConfig';

// Hand-rolled because @grafana/ui's CustomHeadersSettings isn't a top-level export
// before Grafana ~9.4, and this plugin supports >=9.1.1 (it externalizes @grafana/ui,
// so an unavailable export becomes undefined and crashes the whole config page).
// Writes the same standard wire format the SDK reads: jsonData.httpHeaderName{N} +
// secureJsonData.httpHeaderValue{N}, indexed contiguously from 1.

type Props = Pick<DataSourcePluginOptionsEditorProps<PinotConnectionConfig, PinotSecureConfig>, 'options' | 'onOptionsChange'>;

export interface HeaderRow {
  name: string;
  value: string;
  configured: boolean;
}

const NAME_PREFIX = 'httpHeaderName';
const VALUE_PREFIX = 'httpHeaderValue';

export function readHeaderRows(options: Props['options']): HeaderRow[] {
  const jsonData = options.jsonData as unknown as Record<string, unknown>;
  const secureJsonData = (options.secureJsonData || {}) as Record<string, string>;
  const secureJsonFields = (options.secureJsonFields || {}) as Record<string, boolean>;
  const rows: HeaderRow[] = [];
  for (let i = 1; jsonData[`${NAME_PREFIX}${i}`] !== undefined; i++) {
    rows.push({
      name: String(jsonData[`${NAME_PREFIX}${i}`] ?? ''),
      value: secureJsonData[`${VALUE_PREFIX}${i}`] ?? '',
      configured: Boolean(secureJsonFields[`${VALUE_PREFIX}${i}`]),
    });
  }
  return rows;
}

export function withHeaderRows(options: Props['options'], rows: HeaderRow[]): Props['options'] {
  const jsonData = { ...(options.jsonData as unknown as Record<string, unknown>) };
  const secureJsonData = { ...((options.secureJsonData || {}) as Record<string, string>) };
  const secureJsonFields = { ...((options.secureJsonFields || {}) as Record<string, boolean>) };

  // Drop every existing header key, then re-emit a contiguous block from `rows`.
  for (const k of Object.keys(jsonData)) {
    if (k.startsWith(NAME_PREFIX)) {
      delete jsonData[k];
    }
  }
  for (const k of Object.keys(secureJsonData)) {
    if (k.startsWith(VALUE_PREFIX)) {
      delete secureJsonData[k];
    }
  }
  for (const k of Object.keys(secureJsonFields)) {
    if (k.startsWith(VALUE_PREFIX)) {
      delete secureJsonFields[k];
    }
  }

  rows.forEach((row, idx) => {
    const i = idx + 1;
    jsonData[`${NAME_PREFIX}${i}`] = row.name;
    if (row.configured) {
      // Value is already stored server-side; reference it by index.
      // ponytail: removing a saved header above others reindexes these flags but
      // not the server secrets — same limitation as @grafana/ui's own component.
      secureJsonFields[`${VALUE_PREFIX}${i}`] = true;
    } else if (row.value) {
      secureJsonData[`${VALUE_PREFIX}${i}`] = row.value;
    }
  });

  return { ...options, jsonData, secureJsonData, secureJsonFields } as unknown as Props['options'];
}

export function InputCustomHeaders({ options, onOptionsChange }: Props) {
  const rows = readHeaderRows(options);
  const update = (next: HeaderRow[]) => onOptionsChange(withHeaderRows(options, next));

  return (
    <>
      {rows.map((row, idx) => (
        <InlineField key={idx} label={`Header ${idx + 1}`} labelWidth={24}>
          <div style={{ display: 'flex', gap: '4px' }}>
            <Input
              data-testid={`custom-header-name-${idx}`}
              width={20}
              placeholder="X-Custom-Header"
              value={row.name}
              onChange={(e) =>
                update(rows.map((r, i) => (i === idx ? { ...r, name: e.currentTarget.value } : r)))
              }
            />
            <SecretInput
              data-testid={`custom-header-value-${idx}`}
              width={20}
              isConfigured={row.configured}
              placeholder="Header value"
              value={row.value}
              onChange={(e) =>
                update(rows.map((r, i) => (i === idx ? { ...r, value: e.currentTarget.value } : r)))
              }
              onReset={() => update(rows.map((r, i) => (i === idx ? { ...r, value: '', configured: false } : r)))}
            />
            <IconButton
              name="trash-alt"
              tooltip="Remove header"
              data-testid={`custom-header-remove-${idx}`}
              onClick={() => update(rows.filter((_, i) => i !== idx))}
            />
          </div>
        </InlineField>
      ))}
      <Button
        icon="plus"
        variant="secondary"
        data-testid="custom-header-add"
        onClick={() => update([...rows, { name: '', value: '', configured: false }])}
      >
        Add header
      </Button>
    </>
  );
}
