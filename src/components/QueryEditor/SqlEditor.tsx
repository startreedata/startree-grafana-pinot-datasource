import React, { useEffect, useState } from 'react';
import { SQLEditor as GrafanaSqlEditor } from '@grafana/experimental';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SqlEditor(props: { current: string | undefined; onChange: (val: string) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.sqlEditor;

  // The grafana sql editor appears to cache the onChange function in some way (probably unintended?)
  // This work-around uses a local state var for the editor content.

  const [editorContent, setEditorContent] = useState(current || '');
  useEffect(() => {
    if (editorContent !== current) {
      onChange(editorContent);
    }
  });

  return (
    <div className={'gf-form'} data-testid="sql-editor-container">
      <div>
        <FormLabel tooltip={labels.tooltip} label={labels.label} />
      </div>
      <div style={{ flex: '1 1 auto' }}>
        <GrafanaSqlEditor query={editorContent} onChange={setEditorContent}/>
      </div>
    </div>
  );
}
