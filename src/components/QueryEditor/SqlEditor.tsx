import React, { useState } from 'react';
import { SQLEditor as GrafanaSqlEditor } from '@grafana/experimental';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SqlEditor(props: { current: string; onChange: (val: string) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.sqlEditor;

  const [editorContent, setEditorContent] = useState(current);
  return (
    <div className={'gf-form'} data-testid="sql-editor-container">
      <div>
        <FormLabel tooltip={labels.tooltip} label={labels.label} />
      </div>
      <div style={{ flex: '1 1 auto' }} data-testid="sql-editor-content">
        <GrafanaSqlEditor
          query={editorContent}
          onChange={setEditorContent}
          onBlur={() => current !== editorContent && onChange(editorContent)}
        />
      </div>
    </div>
  );
}
