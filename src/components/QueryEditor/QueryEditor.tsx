import React, { useEffect } from 'react';
import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';

import { QueryEditorHeader } from './QueryEditorHeader';
import { QueryType } from '../../dataquery/QueryType';
import { PromQlEditor } from './PromQlEditor';
import { PinotQlEditor } from './PinotQlEditor';
import { isEmpty } from '../../dataquery/PinotDataQuery';
import { fetchDefaultQuery } from '../../resources/defaultQuery';

export function QueryEditor(props: PinotQueryEditorProps) {
  useEffect(() => {
    if (isEmpty(props.query)) {
      fetchDefaultQuery(props.datasource).then((resp) => props.onChange({ ...props.query, ...resp }));
    }
  }, [props.query]);

  return (
    <div>
      <QueryEditorHeader {...props} />
      {(() => {
        switch (props.query.queryType) {
          case QueryType.PromQL:
            return <PromQlEditor {...props} />;
          case QueryType.PinotQL:
          default:
            return <PinotQlEditor {...props} />;
        }
      })()}
    </div>
  );
}
