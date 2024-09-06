import React, { useState } from 'react';
import allLabels from '../../labels';
import { InputTextField } from './InputTextField';

const LimitAuto = -1;

export function InputLimit(props: { current: number | undefined; onChange: (val: number) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.limit;

  const [isValid, setIsValid] = useState<boolean>(true);

  return (
    <div className={'gf-form'} style={{ display: 'flex', flexDirection: 'row' }}>
      <InputTextField
        current={current && current >= 1 ? current.toString(10) : undefined}
        labels={labels}
        invalid={!isValid}
        placeholder={'auto'}
        onChange={(value) => {
          const newLimit = parseLimit(value);
          if (newLimit !== undefined) {
            setIsValid(true);
            onChange(newLimit);
          } else {
            setIsValid(false);
          }
        }}
      />
    </div>
  );
}

function parseLimit(inputData: string | undefined): number | undefined {
  if (!inputData) {
    return LimitAuto;
  }

  const limit = parseInt(inputData, 10);
  switch (true) {
    case limit < 1:
      return undefined;
    case !Number.isFinite(limit):
      return undefined;
    case inputData !== limit.toString(10):
      return undefined;
  }
  return limit;
}
