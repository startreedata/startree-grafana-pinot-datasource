import React from 'react';
import { AccessoryButton } from '@grafana/experimental';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { EditFilter } from './EditFilter';
import { TableSchema } from '../../types/TableSchema';
import { DimensionFilter } from '../../types/DimensionFilter';

export function SelectFilters(props: {
  datasource: DataSource;
  tableSchema: TableSchema | undefined;
  tableName: string | undefined;
  timeColumn: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  dimensionColumns: string[] | undefined;
  dimensionFilters: DimensionFilter[];
  onChange: (val: DimensionFilter[]) => void;
}) {
  const { dimensionColumns, dimensionFilters, onChange } = props;
  const labels = allLabels.components.QueryEditor.filters;

  const filteredColumns = dimensionFilters?.map((f) => f.columnName) || [];
  const unusedColumns = dimensionColumns?.filter((d) => !filteredColumns.includes(d));

  const onChangeFilter = (val: DimensionFilter, idx: number) => {
    onChange(dimensionFilters.map((existing, i) => (i === idx ? val : existing)));
  };
  const onDeleteFilter = (idx: number) => {
    onChange(dimensionFilters.filter((val, i) => i !== idx));
  };

  return (
    <div className={'gf-form'} data-testid="select-filters">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {dimensionFilters.map((filter, idx) => (
          <div key={idx} data-testid="filter-row">
            <EditFilter
              {...props}
              unusedColumns={unusedColumns}
              thisFilter={filter}
              remainingFilters={[...dimensionFilters.slice(0, idx), ...dimensionFilters.slice(idx + 1)]}
              onChange={(val: DimensionFilter) => onChangeFilter(val, idx)}
              onDelete={() => onDeleteFilter(idx)}
            />
          </div>
        ))}
        <div>
          <AccessoryButton
            data-testid="add-filter-btn"
            icon="plus"
            variant="secondary"
            fullWidth={false}
            onClick={() => {
              onChange([...(dimensionFilters || []), {}]);
            }}
          />
        </div>
      </div>
    </div>
  );
}
