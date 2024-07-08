import { Input } from '@grafana/ui';
import React, { ChangeEvent, useState } from 'react';
import { FormLabel } from './FormLabel';
import { styles } from '../styles';
import allLabels from '../labels';

const DefaultLimit = 1_000_000;

export function InputLimit(props: { current: number | undefined; onChange: (val: number) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.limit;

  const [inputData, setInputData] = useState<string | undefined>(current?.toString(10));

  if (current == undefined && current != DefaultLimit) {
    onChange(DefaultLimit);
    setInputData(DefaultLimit.toString(10));
  }

  return (
    <div className={'gf-form'} style={{ display: 'flex', flexDirection: 'row' }}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Input
        className={`${styles.QueryEditor.inputForm}`}
        value={inputData}
        invalid={!parseLimit(inputData)}
        onChange={(event: ChangeEvent<HTMLInputElement>) => {
          const value = event.target.value;
          setInputData(value);
          onChange(parseLimit(value) || DefaultLimit);
        }}
      />
    </div>
  );
}

function parseLimit(inputData: string | undefined): number | undefined {
  if (!inputData) {
    return undefined;
  }

  const limit = parseInt(inputData, 10);
  switch (true) {
    case limit <= 0:
      return undefined;
    case !Number.isFinite(limit):
      return undefined;
    case inputData != limit.toString(10):
      return undefined;
  }
  return limit;
}
