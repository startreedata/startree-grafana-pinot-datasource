import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { SelectDatabase } from './SelectDatabase';
import { SelectTable } from './SelectTable';
import { EditorMode } from '../types/EditorMode';
import { SelectTimeColumn } from './SelectTimeColumn';
import { SelectMetricColumn } from './SelectMetricColumn';
import { SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React from 'react';
import { SqlEditor } from './SqlEditor';

export function PinotQlEditor(props: PinotQueryEditorProps) {
  return (
    <div>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'}>
          <SelectDatabase {...props} />
          <SelectTable {...props} />
        </div>
      </div>
      {props.query.editorMode == EditorMode.Code ? (
        <PinotQlCodeEditor {...props} />
      ) : (
        <PinotQlBuilderEditor {...props} />
      )}
    </div>
  );
}

function PinotQlCodeEditor(props: PinotQueryEditorProps) {
  return (
    <div>
      <SqlEditor {...props} />
    </div>
  );
}

function PinotQlBuilderEditor(props: PinotQueryEditorProps) {
  return (
    <>
      <div>
        <SelectTimeColumn {...props} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetricColumn {...props} />
        <SelectAggregation {...props} />
      </div>
      <div>
        <SelectGroupBy {...props} />
      </div>
      <div>
        <SqlPreview {...props} />
      </div>
    </>
  );
}
