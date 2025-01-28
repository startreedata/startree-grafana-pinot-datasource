import React from 'react';
import { AccessoryButton } from '@grafana/experimental';
import allLabels from '../../labels';
import { QueryOption } from '../../dataquery/QueryOption';
import { PinotQueryOptions } from '../../pinotql/pinotQueryOptions';
import { InlineField } from '@grafana/ui';
import { EditQueryOption } from '../QueryEditor/EditQueryOption';

export function SelectQueryOptions(props: { selected: QueryOption[]; onChange: (val: QueryOption[]) => void }) {
  const { selected, onChange } = props;
  const labels = allLabels.components.QueryEditor.queryOptions;

  const onChangeOption = (val: QueryOption, idx: number) => {
    onChange(selected.map((existing, i) => (i === idx ? val : existing)));
  };

  const onDeleteOption = (idx: number) => {
    onChange(selected.filter((val, i) => i !== idx));
  };

  const selectedNames = selected
    .map(({ name }) => name || '')
    .filter((name) => name)
    .reduce((collector, name) => collector.add(name), new Set<string>());

  const unused = PinotQueryOptions.map(({ name }) => name)
    .filter((name) => !selectedNames.has(name))
    .reduce((collector, name) => collector.add(name), new Set<string>());

  return (
    <InlineField
      data-testid={'select-query-options'}
      label={labels.label}
      labelWidth={24}
      tooltip={labels.tooltip}
      grow
      required
      interactive
    >
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {selected.map((option, idx) => (
          <div key={idx} data-testid="edit-query-option">
            <EditQueryOption
              queryOption={option}
              unused={unused}
              onChange={(val) => onChangeOption(val, idx)}
              onDelete={() => onDeleteOption(idx)}
            />
          </div>
        ))}
        <div>
          <AccessoryButton
            data-testid="add-query-option-btn"
            icon="plus"
            variant="secondary"
            fullWidth={false}
            onClick={() => {
              onChange([...(selected || []), {}]);
            }}
          />
        </div>
      </div>
    </InlineField>
  );
}
