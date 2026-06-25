import { DateTime } from '@grafana/data';
import { RadioButtonGroup } from '@grafana/ui';
import React, { useEffect } from 'react';
import { DataSource } from '../../datasource';
import { SelectTable } from './SelectTable';
import { SelectTimeColumn } from './SelectTimeColumn';
import { SelectFilters } from './SelectFilters';
import { SelectQueryOptions } from './SelectQueryOptions';
import { SelectSpanColumn } from './SelectSpanColumn';
import { InputLimit } from './InputLimit';
import { InputTextField } from './InputTextField';
import { FormLabel } from './FormLabel';
import { SqlPreview } from './SqlPreview';
import allLabels from '../../labels';
import { TracesBuilder } from '../../pinotql';
import { DurationUnits } from '../../pinotql/TracesBuilder';
import { useAutoSurfaceMultiStageEngine } from './useAutoSurfaceMultiStageEngine';

const durationUnitOptions = DurationUnits.map((unit) => ({ label: unit, value: unit }));

export function PinotQlTracesBuilder(props: {
  savedParams: TracesBuilder.Params;
  interpolatedParams: TracesBuilder.Params;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  datasource: DataSource;
  onChange: (value: TracesBuilder.Params) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, datasource, savedParams, interpolatedParams, onChange, onRunQuery } = props;
  const resources = TracesBuilder.useResources(datasource, timeRange, interpolatedParams);
  const labels = allLabels.components.QueryEditor.traces;

  const onChangeAndRun = (newParams: TracesBuilder.Params) => {
    onChange(newParams);
    if (TracesBuilder.canRunQuery(newParams)) {
      onRunQuery();
    }
  };

  useEffect(() => {
    if (TracesBuilder.applyDefaults(savedParams, resources)) {
      onChangeAndRun({ ...savedParams });
    }
  });

  useAutoSurfaceMultiStageEngine(savedParams, interpolatedParams, onChangeAndRun);

  return (
    <>
      <SelectTable
        options={resources.tables}
        selected={savedParams.tableName}
        isLoading={resources.isTablesLoading}
        onChange={(tableName) => onChange({ ...savedParams, tableName })}
      />
      <SelectTimeColumn
        selected={savedParams.timeColumn}
        timeColumns={resources.timeColumns}
        isLoading={resources.isColumnsLoading}
        onChange={(value) => onChangeAndRun({ ...savedParams, timeColumn: value })}
      />
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectSpanColumn
          testId="select-trace-id-column"
          label={labels.traceIdColumn.label}
          tooltip={labels.traceIdColumn.tooltip}
          selected={savedParams.traceIdColumn}
          columns={resources.stringColumns}
          isLoading={resources.isColumnsLoading}
          invalid={!savedParams.traceIdColumn.name}
          onChange={(traceIdColumn) => onChangeAndRun({ ...savedParams, traceIdColumn })}
        />
        <SelectSpanColumn
          testId="select-span-id-column"
          label={labels.spanIdColumn.label}
          tooltip={labels.spanIdColumn.tooltip}
          selected={savedParams.spanIdColumn}
          columns={resources.stringColumns}
          isLoading={resources.isColumnsLoading}
          invalid={!savedParams.spanIdColumn.name}
          onChange={(spanIdColumn) => onChangeAndRun({ ...savedParams, spanIdColumn })}
        />
        <SelectSpanColumn
          testId="select-parent-span-id-column"
          label={labels.parentSpanIdColumn.label}
          tooltip={labels.parentSpanIdColumn.tooltip}
          selected={savedParams.parentSpanIdColumn}
          columns={resources.stringColumns}
          isLoading={resources.isColumnsLoading}
          onChange={(parentSpanIdColumn) => onChangeAndRun({ ...savedParams, parentSpanIdColumn })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectSpanColumn
          testId="select-service-name-column"
          label={labels.serviceNameColumn.label}
          tooltip={labels.serviceNameColumn.tooltip}
          selected={savedParams.serviceNameColumn}
          columns={resources.stringColumns}
          isLoading={resources.isColumnsLoading}
          onChange={(serviceNameColumn) => onChangeAndRun({ ...savedParams, serviceNameColumn })}
        />
        <SelectSpanColumn
          testId="select-span-name-column"
          label={labels.spanNameColumn.label}
          tooltip={labels.spanNameColumn.tooltip}
          selected={savedParams.spanNameColumn}
          columns={resources.stringColumns}
          isLoading={resources.isColumnsLoading}
          onChange={(spanNameColumn) => onChangeAndRun({ ...savedParams, spanNameColumn })}
        />
        <SelectSpanColumn
          testId="select-status-column"
          label={labels.statusColumn.label}
          tooltip={labels.statusColumn.tooltip}
          selected={savedParams.statusColumn}
          columns={resources.stringColumns}
          isLoading={resources.isColumnsLoading}
          onChange={(statusColumn) => onChangeAndRun({ ...savedParams, statusColumn })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectSpanColumn
          testId="select-duration-column"
          label={labels.durationColumn.label}
          tooltip={labels.durationColumn.tooltip}
          selected={savedParams.durationColumn}
          columns={resources.numericColumns}
          isLoading={resources.isColumnsLoading}
          invalid={!savedParams.durationColumn.name}
          onChange={(durationColumn) => onChangeAndRun({ ...savedParams, durationColumn })}
        />
        <div className={'gf-form'} data-testid="select-duration-unit">
          <FormLabel tooltip={labels.durationUnit.tooltip} label={labels.durationUnit.label} />
          <RadioButtonGroup
            options={durationUnitOptions}
            value={savedParams.durationUnit}
            onChange={(durationUnit) => onChangeAndRun({ ...savedParams, durationUnit })}
          />
        </div>
        <SelectSpanColumn
          testId="select-tags-column"
          label={labels.tagsColumn.label}
          tooltip={labels.tagsColumn.tooltip}
          selected={savedParams.tagsColumn}
          columns={resources.tagsColumns}
          isLoading={resources.isColumnsLoading}
          onChange={(tagsColumn) => onChangeAndRun({ ...savedParams, tagsColumn })}
        />
      </div>
      <div className={'gf-form'} data-testid="input-trace-id">
        <InputTextField
          current={savedParams.traceId}
          labels={labels.traceId}
          placeholder={labels.traceId.placeholder}
          onChange={(traceId) => onChangeAndRun({ ...savedParams, traceId })}
        />
      </div>
      <SelectFilters
        datasource={datasource}
        tableName={savedParams.tableName}
        timeColumn={savedParams.timeColumn}
        timeRange={timeRange}
        columns={resources.filterColumns}
        filters={savedParams.filters}
        isColumnsLoading={resources.isColumnsLoading}
        onChange={(val) => onChangeAndRun({ ...savedParams, filters: val })}
      />
      <SelectQueryOptions
        selected={savedParams.queryOptions}
        onChange={(queryOptions) => onChangeAndRun({ ...savedParams, queryOptions })}
      />
      <InputLimit current={savedParams.limit} onChange={(limit) => onChangeAndRun({ ...savedParams, limit })} />
      <SqlPreview sql={resources.sqlPreview} />
    </>
  );
}
