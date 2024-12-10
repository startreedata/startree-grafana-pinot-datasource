import { ConfirmModal } from './ConfirmModal';
import { EditorMode } from '../../types/EditorMode';
import { RadioButtonGroup } from '@grafana/ui';
import React, { useState } from 'react';
import { groupByColumnsFrom, PinotDataQuery } from '../../types/PinotDataQuery';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { DisplayTypeTimeSeries } from './SelectDisplayType';
import { previewSqlBuilder } from '../../resources/previewSql';
import { QueryType } from '../../types/QueryType';

export function SelectEditorMode(props: {
  query: PinotDataQuery;
  datasource: DataSource;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  onChange: (value: PinotDataQuery) => void;
}) {
  const { query, datasource, intervalSize, timeRange, onChange } = props;
  const [showConfirm, setShowConfirm] = useState(false);

  return (
    <div data-testid="select-editor-mode">
      <ConfirmModal
        isOpen={showConfirm}
        onCopy={() => {
          setShowConfirm(false);
          navigator.clipboard.writeText(query.pinotQlCode || '').then(() =>
            onChange({
              ...query,
              editorMode: EditorMode.Builder,
              pinotQlCode: undefined,
            })
          );
        }}
        onDiscard={() => {
          setShowConfirm(false);
          onChange({
            ...query,
            editorMode: EditorMode.Builder,
            pinotQlCode: undefined,
          });
        }}
        onCancel={() => setShowConfirm(false)}
      />
      {query.queryType === QueryType.PinotQL && (
        <RadioButtonGroup
          options={Object.keys(EditorMode).map((name) => ({ label: name, value: name }))}
          onChange={(value) => {
            if (value === EditorMode.Builder) {
              setShowConfirm(true);
            }

            if (value === EditorMode.Code) {
              previewSqlBuilder(datasource, {
                intervalSize: intervalSize,
                timeRange: timeRange,
                expandMacros: false,
                aggregationFunction: query.aggregationFunction,
                groupByColumns: groupByColumnsFrom(query),
                metricColumn: query.metricColumn,
                tableName: query.tableName,
                timeColumn: query.timeColumn,
                filters: query.filters,
                limit: query.limit,
                granularity: query.granularity,
                orderBy: query.orderBy,
                queryOptions: query.queryOptions,
              }).then((sql) =>
                onChange({
                  ...query,
                  editorMode: EditorMode.Code,
                  displayType: DisplayTypeTimeSeries,
                  timeColumnAlias: 'time',
                  metricColumnAlias: query.metricColumn,
                  pinotQlCode:
                    sql ||
                    `SELECT
                                                             $__timeGroup("timestamp") AS $__timeAlias(), SUM("metric") AS $__metricAlias()
                                                         FROM $__table()
                                                         WHERE $__timeFilter("timestamp")
                                                         GROUP BY $__timeGroup("timestamp")
                                                         ORDER BY $__timeAlias() DESC
                                                             LIMIT 100000`,
                })
              );
            }
          }}
          value={query.editorMode}
        />
      )}
    </div>
  );
}
