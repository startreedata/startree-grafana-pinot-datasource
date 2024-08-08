import React, { ChangeEvent, useEffect, useState } from 'react';
import { FormLabel } from './FormLabel';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { Input, Select } from '@grafana/ui';
import { styles } from '../../styles';
import allLabels from '../../labels';
import { QueryOption } from '../../types/QueryOption'; // I'm currently trying to decide between different experience for query options

// ref https://docs.pinot.apache.org/users/user-guide-query/query-options

const QueryOptionChoices = [
  { name: 'timeoutMs' },
  { name: 'enableNullHandling' },
  { name: 'explainPlanVerbose' },
  { name: 'useMultistageEngine' },
  { name: 'maxExecutionThreads' },
  { name: 'numReplicaGroupsToQuery' },
  { name: 'minSegmentGroupTrimSize' },
  { name: 'minServerGroupTrimSize' },
  { name: 'skipIndexes' },
  { name: 'skipUpsert' },
  { name: 'useStarTree' },
  { name: 'maxRowsInJoin' },
  { name: 'inPredicatePreSorted' },
  { name: 'inPredicateLookupAlgorithm' },
  { name: 'maxServerResponseSizeBytes' },
  { name: 'maxQueryResponseSizeBytes' },
];

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

  const unused = QueryOptionChoices.map(({ name }) => name)
    .filter((name) => !selectedNames.has(name))
    .reduce((collector, name) => collector.add(name), new Set<string>());

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {selected.map((option, idx) => (
          <EditQueryOption
            key={idx}
            queryOption={option}
            unused={unused}
            onChange={(val) => onChangeOption(val, idx)}
            onDelete={() => onDeleteOption(idx)}
          />
        ))}
        <div>
          <AccessoryButton
            icon="plus"
            variant="secondary"
            fullWidth={false}
            onClick={() => {
              onChange([...(selected || []), {}]);
            }}
          />
        </div>
      </div>
    </div>
  );
}

function EditQueryOption(props: {
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
