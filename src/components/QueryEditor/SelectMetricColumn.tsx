import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { Column } from '../../resources/columns';
import { ComplexField } from '../../dataquery/ComplexField';
import { formDataOf } from '../../pinotql/complexField';

export function SelectMetricColumn(props: {
  selected: ComplexField;
  metricColumns: Column[];
  isCount: boolean;
  isLoading: boolean;
  onChange: (val: ComplexField) => void;
}) {
  const { isCount, selected, metricColumns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.metricColumn;
  const formData = formDataOf(selected, metricColumns);
  return (
    <div className={'gf-form'} data-testid="select-metric-column">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div data-testid="select-metric-column-dropdown">
        <Select
          className={`${styles.QueryEditor.inputForm}`}
          allowCustomValue
          invalid={!selected}
          isLoading={isLoading}
          options={isCount ? [{ label: '*', value: '*' }] : formData.options}
          value={isCount ? '*' : formData.usedOption}
          disabled={isCount}
          onChange={(item) => onChange(formData.getChange(item))}
        />
      </div>
    </div>
  );
}
