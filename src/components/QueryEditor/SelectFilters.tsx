import React from 'react';
import { AccessoryButton } from '@grafana/experimental';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { EditFilter } from './EditFilter';
import { DimensionFilter } from '../../types/DimensionFilter';
import { columnLabelOf } from '../../types/ComplexField';
import { Column } from '../../resources/columns';

export function SelectFilters(props: {
  datasource: DataSource;
  tableName: string | undefined;
  timeColumn: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  dimensionColumns: Column[] | undefined;
  dimensionFilters: DimensionFilter[];
  onChange: (val: DimensionFilter[]) => void;
}) {
  const labels = allLabels.components.QueryEditor.filters;

  const { datasource, dimensionColumns, dimensionFilters, tableName, timeColumn, timeRange, onChange } = props;
  const filteredColumns = (dimensionFilters || [])
    .filter(({ columnName }) => columnName)
    .map((f) => columnLabelOf(f.columnName || '', f.columnKey));
  const unusedColumns =
    dimensionColumns?.filter(({ name, key }) => !filteredColumns.includes(columnLabelOf(name, key))) || [];

  const onChangeFilter = (val: DimensionFilter, idx: number) => {
    onChange(dimensionFilters.map((existing, i) => (i === idx ? val : existing)));
  };
  const onDeleteFilter = (idx: number) => {
    onChange(dimensionFilters.filter((_val, i) => i !== idx));
  };

  return (
    <div className={'gf-form'} data-testid="select-filters">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {dimensionFilters.map((filter, idx) => (
          <div key={idx} data-testid="filter-row">
            <EditFilter
              datasource={datasource}
              tableName={tableName}
              timeColumn={timeColumn}
              timeRange={timeRange}
              unusedColumns={unusedColumns}
              thisColumn={dimensionColumns?.find(
                ({ name, key }) => filter.columnName == name && filter.columnKey == key
              )}
              thisFilter={filter}
              isLoadingColumns={dimensionColumns === undefined}
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
