import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel, Input } from '@grafana/ui';
import { styles } from '../styles';
import React, { ChangeEvent, useState } from 'react';

const DefaultLimit = 1_000_000;

export function InputLimit(props: PinotQueryEditorProps) {
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
    <div style={{ display: 'flex', flexDirection: 'row' }}>
      <InlineFormLabel width={8} className="query-keyword">
        Limit
      </InlineFormLabel>
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
