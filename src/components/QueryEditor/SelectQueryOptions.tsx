import React from 'react';
import { FormLabel } from './FormLabel';
import { AccessoryButton } from '@grafana/experimental';
import allLabels from '../../labels';
import { QueryOption } from '../../types/QueryOption';
import { EditQueryOption } from './EditQueryOption';

// ref https://docs.pinot.apache.org/users/user-guide-query/query-options
// TODO: Is there a pinot api that provides this list?

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
