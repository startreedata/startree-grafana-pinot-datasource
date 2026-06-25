import { ComplexField } from '../../dataquery/ComplexField';
import { Column } from '../../resources/columns';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { formDataOf } from '../../pinotql/complexField';

// SelectSpanColumn is the shared column picker for the trace builder's span roles (trace id, span
// id, service, duration, tags, ...). It's a thin wrapper over the complex-field Select used
// everywhere else; the role differs only by label/tooltip/column set, so one component covers all.
export function SelectSpanColumn(props: {
  testId: string;
  label: string;
  tooltip: string;
  selected: ComplexField;
  columns: Column[];
  isLoading: boolean;
  invalid?: boolean;
  onChange: (val: ComplexField) => void;
}) {
  const { testId, label, tooltip, selected, columns, isLoading, invalid, onChange } = props;
  const formData = formDataOf(selected, columns);
  return (
    <div className={'gf-form'} data-testid={testId}>
      <FormLabel tooltip={tooltip} label={label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        isClearable
        invalid={invalid}
        isLoading={isLoading}
        options={formData.options}
        value={formData.usedOption}
        onChange={(item) => onChange(item ? formData.getChange(item) : {})}
      />
    </div>
  );
}
