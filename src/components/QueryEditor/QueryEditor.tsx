import React from 'react';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';

import { SelectEditorType } from './SelectEditorType';
import { QueryType } from '../../types/QueryType';
import { PinotQlEditor } from './PinotQlEditor';

export function QueryEditor(props: PinotQueryEditorProps) {
  return (
    <div>
      <SelectEditorType {...props} />
      {(() => {
        switch (props.query.queryType) {
          case QueryType.PinotQL:
          default:
            return <PinotQlEditor {...props} />;
        }
      })()}
    </div>
  );
}
