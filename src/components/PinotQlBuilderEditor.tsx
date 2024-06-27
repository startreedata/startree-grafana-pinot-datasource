import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { SelectTimeColumn } from './SelectTimeColumn';
import { SelectMetricColumn } from './SelectMetricColumn';
import { SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React from 'react';

export function PinotQlBuilderEditor(props: PinotQueryEditorProps) {
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
