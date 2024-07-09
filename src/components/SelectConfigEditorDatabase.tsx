import { InlineField, Select } from '@grafana/ui';
import React, { useEffect, useState } from 'react';

export function SelectConfigEditorDatabase(props: {
  selected: string | undefined;
  tokenType: string | undefined;
  tokenValue: string | undefined;
  onChange: (val: string) => void;
}) {
  const { selected, tokenType, tokenValue, onChange } = props;
  const defaultValue = 'default';

  const options = useConfigEditorDatabases(selected, tokenType, tokenValue);

  if (options?.length === 0 && selected === undefined) {
    onChange(defaultValue);
  } else if (options?.length === 1 && selected === undefined) {
    onChange(options[0]);
  }

  if (options?.length === 0) {
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
