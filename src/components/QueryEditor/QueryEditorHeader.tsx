import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { ToolbarButton } from '@grafana/ui';
import React from 'react';
import { QueryType } from '../../types/QueryType';
import { EditorMode } from '../../types/EditorMode';
import { SelectQueryType } from './SelectQueryType';
import { SelectEditorMode } from './SelectEditorMode';

export function QueryEditorHeader(props: PinotQueryEditorProps) {
  const { query, onChange, onRunQuery } = props;

  if (query.queryType === undefined || query.editorMode === undefined) {
    onChange({
      ...query,
      queryType: query.queryType || QueryType.PinotQL,
      editorMode: query.editorMode || EditorMode.Builder,
    });
  }

  return (
    <div style={{ display: 'flex', justifyContent: 'space-between' }} data-testid="query-editor-header">
      <div className={'gf-form'} data-testid="select-query-type">
        <SelectQueryType current={query.queryType} onChange={(queryType) => onChange({ ...query, queryType })} />
      </div>

      <div style={{ display: 'flex' }}>
        <div className={'gf-form'}>
          <SelectEditorMode
            datasource={props.datasource}
            query={props.query}
            onChange={props.onChange}
            timeRange={{
              to: props.data?.request?.range.to,
              from: props.data?.request?.range.from,
            }}
            intervalSize={props.data?.request?.interval}
          />

          <ToolbarButton
            data-testid="run-query-btn"
            icon={'play'}
            variant={'primary'}
            style={{ marginLeft: 4 }}
            onClick={() => onRunQuery()}
          >
            Run Query
          </ToolbarButton>
        </div>
      </div>
    </div>
  );
}
