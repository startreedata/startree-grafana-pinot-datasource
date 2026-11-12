import React from 'react';
import { render } from '@testing-library/react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { ConfigEditor } from './ConfigEditor';
import { InputPinotToken } from './InputPinotToken';
import { PinotConnectionConfig } from '../../config/PinotConnectionConfig';

jest.mock('./InputPinotToken', () => ({ InputPinotToken: jest.fn(() => null) }));

// Grafana 10.4 UI components use React 18's useId; the plugin also supports React 17.
if (!(React as { useId?: unknown }).useId) {
  (React as unknown as { useId: () => string }).useId = () => 'config-editor-test';
}

test.each([
  [{ authToken: true }, true],
  [{ apiKey: true }, false],
])('reads and resets the stored authToken for %j', (secureJsonFields, isConfigured) => {
  const options = {
    jsonData: { queryOptions: [], tokenType: 'Bearer' },
    secureJsonData: {},
    secureJsonFields,
  } as unknown as DataSourcePluginOptionsEditorProps<PinotConnectionConfig>['options'];

  const onOptionsChange = jest.fn();
  render(<ConfigEditor options={options} onOptionsChange={onOptionsChange} />);

  expect(InputPinotToken).toHaveBeenLastCalledWith(expect.objectContaining({ isConfigured }), expect.anything());

  const calls = jest.mocked(InputPinotToken).mock.calls;
  calls[calls.length - 1][0].onResetToken();
  expect(onOptionsChange).toHaveBeenCalledWith(
    expect.objectContaining({
      secureJsonFields: expect.objectContaining({ authToken: false }),
      secureJsonData: { authToken: '' },
    })
  );
});

test.each([
  [undefined, false],
  ['off', false],
  ['on', true],
  ['login', true],
  ['email', true],
])('shows query attribution %s as switched on: %s', (queryAttribution, on) => {
  const options = {
    jsonData: { queryOptions: [], queryAttribution },
    secureJsonData: {},
    secureJsonFields: {},
  } as unknown as DataSourcePluginOptionsEditorProps<PinotConnectionConfig>['options'];

  const { getByTestId } = render(<ConfigEditor options={options} onOptionsChange={jest.fn()} />);
  const toggle = getByTestId('toggle-query-attribution').querySelector('input[type="checkbox"]') as HTMLInputElement;
  expect(toggle.checked).toBe(on);
});
