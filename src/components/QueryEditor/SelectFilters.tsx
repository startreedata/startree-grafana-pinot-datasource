import React from 'react';
import { AccessoryButton } from '@grafana/experimental';
import { DataSource } from '../../datasource';
import { TimeRange } from '@grafana/data';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { EditFilter } from './EditFilter';
import { TableSchema } from '../../types/TableSchema';
import { DimensionFilter } from '../../types/DimensionFilter';

export function SelectFilters(props: {
  datasource: DataSource;
  databaseName: string | undefined;
  tableSchema: TableSchema | undefined;
  tableName: string | undefined;
  timeColumn: string | undefined;
  range: TimeRange | undefined;
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
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {dimensionFilters.map((filter, idx) => (
          <EditFilter
            {...props}
            key={idx}
            unusedColumns={unusedColumns}
            thisFilter={filter}
            remainingFilters={[...dimensionFilters.slice(0, idx), ...dimensionFilters.slice(idx + 1)]}
            onChange={(val: DimensionFilter) => onChangeFilter(val, idx)}
            onDelete={() => onDeleteFilter(idx)}
          />
        ))}
        <div>
          <AccessoryButton
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
