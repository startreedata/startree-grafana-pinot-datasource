import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';
import { ToolbarButton, useTheme2 } from '@grafana/ui';
import React from 'react';
import { SelectQueryType } from './SelectQueryType';
import { SelectEditorMode } from './SelectEditorMode';
import { QueryType } from '../../dataquery/QueryType';

export function QueryEditorHeader(props: PinotQueryEditorProps) {
  const { query, onChange, onRunQuery } = props;

  const theme = useTheme2();
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between' }} data-testid="query-editor-header">
      <div className={'gf-form'} data-testid="select-query-type">
        <SelectQueryType
          current={query.queryType || QueryType.PinotQL}
          onChange={(queryType) => onChange({ ...query, queryType, tableName: undefined })}
        />
      </div>

      <div style={{ display: 'flex' }}>
        <div className={'gf-form'}>
          <SelectEditorMode
            datasource={props.datasource}
            query={props.query}
            onChange={props.onChange}
            timeRange={{
              to: props.range?.to,
              from: props.range?.from,
            }}
            intervalSize={props.data?.request?.interval}
          />

          <ToolbarButton
            data-testid="run-query-btn"
            icon={'play'}
            variant={'primary'}
            style={{ marginLeft: theme.spacing(0.5) }}
            onClick={() => onRunQuery()}
          >
            Run Query
          </ToolbarButton>
        </div>
      </div>
    </div>
  );
}
