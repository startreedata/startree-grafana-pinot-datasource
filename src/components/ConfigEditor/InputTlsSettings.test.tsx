import React from 'react';
// This plugin runs on React 17, but @grafana/ui 10.4 components (Tooltip) call
// React.useId (React 18). Polyfill it so the components render under jsdom.
if (!(React as { useId?: unknown }).useId) {
  let n = 0;
  (React as unknown as { useId: () => string }).useId = () => `test-id-${n++}`;
}
import { render, screen, fireEvent } from '@testing-library/react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InputTlsSettings } from './InputTlsSettings';
import { PinotConnectionConfig, PinotSecureConfig } from '../../config/PinotConnectionConfig';

type Props = DataSourcePluginOptionsEditorProps<PinotConnectionConfig, PinotSecureConfig>;

function renderWith(jsonData: Partial<PinotConnectionConfig>) {
  const onOptionsChange = jest.fn();
  const options = {
    jsonData: { queryOptions: [], ...jsonData },
    secureJsonData: {},
    secureJsonFields: {},
  } as unknown as Props['options'];
  render(<InputTlsSettings options={options} onOptionsChange={onOptionsChange} />);
  return { onOptionsChange };
}

test('skip-verify toggle writes the standard tlsSkipVerify jsonData key', () => {
  const { onOptionsChange } = renderWith({});
  fireEvent.click(screen.getByTestId('switch-tls-skip-verify'));
  expect(onOptionsChange).toHaveBeenCalledWith(
    expect.objectContaining({ jsonData: expect.objectContaining({ tlsSkipVerify: true }) })
  );
});

test('server name field is hidden by default', () => {
  renderWith({});
  expect(screen.queryByTestId('input-tls-server-name')).toBeNull();
});

test('server name field appears once a TLS auth mode is enabled', () => {
  renderWith({ tlsAuth: true });
  expect(screen.queryByTestId('input-tls-server-name')).not.toBeNull();
});
