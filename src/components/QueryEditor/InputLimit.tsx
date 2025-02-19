import React, { useState } from 'react';
import allLabels from '../../labels';
import { InputTextField } from './InputTextField';

const LimitAuto = -1;

export function InputSeriesLimit(props: { current: number; onChange: (val: number) => void }) {
  return (
    <div className={'gf-form'} data-testid="input-series-limit">
      <InputLimitForm {...props} labels={allLabels.components.QueryEditor.seriesLimit} />
    </div>
  );
}

export function InputLimit(props: { current: number; onChange: (val: number) => void }) {
  return (
    <div className={'gf-form'} data-testid="input-limit">
      <InputLimitForm {...props} labels={allLabels.components.QueryEditor.limit} />
    </div>
  );
}

function InputLimitForm(props: {
  current: number;
  onChange: (val: number) => void;
  labels: { label: string; tooltip: string };
}) {
  const { current, onChange, labels } = props;

  const [limitText, setLimitText] = useState<string>(current >= 1 ? current.toString(10) : '');
  const [isValid, setIsValid] = useState<boolean>(true);

  return (
    <InputTextField
      current={limitText}
      labels={labels}
      invalid={!isValid}
      placeholder={'auto'}
      onChange={(value) => {
        setLimitText(value);
        const [newLimit, valid] = parseLimit(value);
        setIsValid(valid);
        onChange(newLimit);
      }}
    />
  );
}

function parseLimit(inputData: string | undefined): [number, boolean] {
  if (!inputData) {
    return [LimitAuto, true];
  }

  const limit = parseInt(inputData, 10);
  switch (true) {
    case limit < 1:
    case !Number.isFinite(limit):
    case inputData !== limit.toString(10):
      return [LimitAuto, false];
    default:
      return [limit, true];
  }
}
