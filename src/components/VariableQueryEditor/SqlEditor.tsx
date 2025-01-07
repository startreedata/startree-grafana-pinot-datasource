import { InlineFormLabel } from '@grafana/ui';
import { SQLEditor as GrafanaSqlEditor } from '@grafana/experimental';
import React, { useEffect, useState } from 'react';
import allLabels from '../../labels';

export function SqlEditor(props: { current: string | undefined; onChange: (val: string) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.VariableQueryEditor.sqlEditor;

  // The grafana sql editor appears to cache the onChange function in some way (probably unintended?)
  // This work-around uses a local state var for the editor content.

  const [editorContent, setEditorContent] = useState(current || '');
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      if (editorContent !== current) {
        onChange(editorContent);
      }
    }, 300);
    return () => clearTimeout(timeoutId);
  }, [editorContent, current, onChange]);

  return (
    <>
      <InlineFormLabel width={10} tooltip={labels.tooltip} data-testid="inline-form-label">
        {labels.label}
      </InlineFormLabel>
      <div style={{ flex: '1 1 auto' }} data-testid={'sql-editor-content'}>
        <GrafanaSqlEditor query={editorContent} onChange={setEditorContent} />
      </div>
    </>
  );
}
