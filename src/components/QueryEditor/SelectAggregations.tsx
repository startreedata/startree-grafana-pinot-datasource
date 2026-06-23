import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { Column } from '../../resources/columns';
import { Aggregation } from '../../dataquery/Aggregation';
import { AccessoryButton } from '@grafana/experimental';
import { EditAggregation } from './EditAggregation';
import { AggregationFunction } from './SelectAggregation';

export function SelectAggregations(props: {
  aggregations: Aggregation[];
  columns: Column[];
  isLoadingColumns: boolean;
  onChange: (val: Aggregation[]) => void;
}) {
  const labels = allLabels.components.QueryEditor.aggregations;

  const { aggregations, columns, isLoadingColumns, onChange } = props;

  const onChangeField = (val: Aggregation, idx: number) => {
    onChange(aggregations?.map((existing, i) => (i === idx ? val : existing)));
  };
  const onDeleteField = (idx: number) => {
    onChange(aggregations?.filter((_val, i) => i !== idx));
  };

  return (
    <div className={'gf-form'} data-testid="select-aggregations">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {aggregations?.map((aggregation, idx) => (
          <EditAggregation
            key={idx}
            aggregation={aggregation}
            columns={columns}
            isLoadingColumns={isLoadingColumns}
            onChange={(val) => onChangeField(val, idx)}
            onDelete={() => onDeleteField(idx)}
          />
        ))}
        <div>
          <AccessoryButton
            data-testid="add-aggregation-btn"
            icon="plus"
            variant="secondary"
            fullWidth={false}
            onClick={() => onChange([...(aggregations || []), { function: AggregationFunction.COUNT, column: {} }])}
          />
        </div>
      </div>
    </div>
  );
}
