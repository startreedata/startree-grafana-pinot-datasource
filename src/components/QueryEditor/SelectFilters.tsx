import React from 'react';
import { AccessoryButton } from '@grafana/experimental';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { EditFilter } from './EditFilter';
import { DimensionFilter } from '../../dataquery/DimensionFilter';
import { Column } from '../../resources/columns';
import { columnLabelOf } from '../../pinotql/complexField';

export function SelectFilters(props: {
  datasource: DataSource;
  tableName: string;
  timeColumn: string;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  columns: Column[];
  filters: DimensionFilter[];
  isColumnsLoading: boolean;
  onChange: (val: DimensionFilter[]) => void;
}) {
  const labels = allLabels.components.QueryEditor.filters;

  const { datasource, columns, filters, tableName, timeColumn, timeRange, isColumnsLoading, onChange } = props;

  // TODO: extract logic to a separate function

  const filteredColumns = filters
    .filter(({ columnName }) => columnName)
    .map((f) => columnLabelOf(f.columnName || '', f.columnKey));
  const unusedColumns = columns?.filter(({ name, key }) => !filteredColumns.includes(columnLabelOf(name, key))) || [];

  const onChangeFilter = (val: DimensionFilter, idx: number) => {
    onChange(filters.map((existing, i) => (i === idx ? val : existing)));
  };
  const onDeleteFilter = (idx: number) => {
    onChange(filters.filter((_val, i) => i !== idx));
  };

  return (
    <div className={'gf-form'} data-testid="select-filters">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {filters.map((filter, idx) => (
          <div key={idx} data-testid="edit-filter">
            <EditFilter
              datasource={datasource}
              tableName={tableName}
              timeColumn={timeColumn}
              timeRange={timeRange}
              unusedColumns={unusedColumns}
              thisColumn={columns?.find(({ name, key }) => filter.columnName === name && filter.columnKey === key)}
              thisFilter={filter}
              isLoadingColumns={isColumnsLoading}
              remainingFilters={[...filters.slice(0, idx), ...filters.slice(idx + 1)]}
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
              onChange([...(filters || []), {}]);
            }}
          />
        </div>
      </div>
    </div>
  );
}
