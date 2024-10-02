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
  }, [value, queryOption, onChange]);

  const selectableNames = queryOption.name ? [queryOption.name, ...unused] : [...unused];
  return (
    <InputGroup>
      <div style={{ padding: 6 }} data-testid="set-label">
        <span>SET</span>
      </div>
      <Select
        id="query-option-select"
        width="auto"
        value={queryOption.name}
        allowCustomValue
        options={selectableNames.map((name) => ({ label: name, value: name }))}
        onChange={(change) => onChange({ ...queryOption, name: change.value })}
      />
      <div style={{ padding: 6 }} data-testid="operator-label">
        <span>=</span>
      </div>
      <Input
        id="query-option-value-input"
        className={`${styles.QueryEditor.inputForm}`}
        value={value}
        onChange={(event: ChangeEvent<HTMLInputElement>) => setValue(event.target.value)}
      />
      <AccessoryButton data-testid="delete-query-option-btn" icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
