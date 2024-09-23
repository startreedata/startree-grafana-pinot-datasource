import { FormLabel } from './FormLabel';
import { RadioButtonGroup } from '@grafana/ui';
import { QueryType } from '../../types/QueryType';
import React from 'react';
import allLabels from '../../labels';

const SupportedQueryTypes = [QueryType.PinotQL];

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
        data-testid="radio-btn-group"
        options={SupportedQueryTypes.map((name) => ({ label: name, value: name }))}
        onChange={(value) => {
          // Manually disable unimplemented options
          switch (value) {
            case QueryType.LogQL:
            case QueryType.PromQL:
              // TODO: Add some unsupported popup
              return;
          }
          onChange(value);
        }}
        value={current}
      />
    </>
  );
}
