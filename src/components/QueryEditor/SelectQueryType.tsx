import { FormLabel } from './FormLabel';
import { RadioButtonGroup } from '@grafana/ui';
import { QueryType } from '../../dataquery/QueryType';
import React from 'react';
import allLabels from '../../labels';

const SupportedQueryTypes = [QueryType.PinotQL, QueryType.PromQL];

export function SelectQueryType({
  current,
  onChange,
}: {
  current: string | undefined;
  onChange: (val: string) => void;
}) {
  const labels = allLabels.components.QueryEditor.queryType;

  return (
    <>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <RadioButtonGroup
        data-testid="select-query-type"
        options={SupportedQueryTypes.map((name) => ({ label: name, value: name }))}
        onChange={onChange}
        value={current}
      />
    </>
  );
}
