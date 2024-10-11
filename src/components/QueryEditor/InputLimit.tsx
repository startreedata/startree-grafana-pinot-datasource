import React, { useState } from 'react';
import allLabels from '../../labels';
import { InputTextField } from './InputTextField';

const LimitAuto = -1;

export function InputLimit(props: { current: number | undefined; onChange: (val: number) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.limit;

  const [limitText, setLimitText] = useState<string>(current && current >= 1 ? current?.toString(10) : '');
  const [isValid, setIsValid] = useState<boolean>(true);

  return (
    <div className={'gf-form'} style={{ display: 'flex', flexDirection: 'row' }}>
      <InputTextField
        data-testid="input-limit"
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
    </div>
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
