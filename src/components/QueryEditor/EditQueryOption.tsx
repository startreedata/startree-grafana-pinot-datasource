import React, { ChangeEvent, useEffect, useState } from 'react';
import { QueryOption } from '../../types/QueryOption';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { Input, Select } from '@grafana/ui';
import { styles } from '../../styles';

export function EditQueryOption(props: {
  queryOption: QueryOption;
  unused: Set<string>;
  onDelete: () => void;
  onChange: (val: QueryOption) => void;
}) {
  const { queryOption, unused, onChange, onDelete } = props;

  const [value, setValue] = useState(queryOption.value);

  useEffect(() => {
    const timeoutId = setTimeout(() => queryOption.value !== value && onChange({ ...queryOption, value }), 500);
    return () => clearTimeout(timeoutId);
  }, [value]);

  const selectableNames = queryOption.name ? [queryOption.name, ...unused] : [...unused];
  return (
    <InputGroup>
      <div style={{ padding: 6 }}>
        <span>SET</span>
      </div>
      <Select
        width="auto"
        value={queryOption.name}
        allowCustomValue
        options={selectableNames.map((name) => ({ label: name, value: name }))}
        onChange={(change) => onChange({ ...queryOption, name: change.value })}
      />
      <div style={{ padding: 6 }}>
        <span>=</span>
      </div>
      <Input
        className={`${styles.QueryEditor.inputForm}`}
        value={value}
        onChange={(event: ChangeEvent<HTMLInputElement>) => setValue(event.target.value)}
      />
      <AccessoryButton icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
