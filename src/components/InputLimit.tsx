import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { Input } from '@grafana/ui';
import { styles } from '../styles';
import React, { ChangeEvent, useState } from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

const DefaultLimit = 1_000_000;

export function InputLimit(props: PinotQueryEditorProps) {
  const labels = allLabels.components.QueryEditor.limit;

  const [inputData, setInputData] = useState<string | undefined>(DefaultLimit.toString(10));
  const [isValid, setIsValid] = useState<boolean>(true);
  const { query, onChange } = props;

  const onChangeLimit = (event: ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setInputData(value);

    const limit = parseInt(value, 10);
    const newIsValid = value == limit.toString(10) && Number.isFinite(limit) && limit > 0;
    setIsValid(newIsValid);
    onChange({ ...query, limit: newIsValid ? limit : DefaultLimit });
  };

  return (
    <div className={'gf-form'} style={{ display: 'flex', flexDirection: 'row' }}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Input
        className={`width-15 ${styles.Common.inlineSelect}`}
        onChange={onChangeLimit}
        value={inputData}
        invalid={!isValid}
        width={24}
      />
    </div>
  );
}
