import React from 'react';
import { PinotVariableQuery } from '../../types/PinotVariableQuery';
import { SqlEditor } from './SqlEditor';

export function SqlVariableEditor({
  selectTable,
  variableQuery,
  onChange,
}: {
  selectTable: React.JSX.Element;
  variableQuery: PinotVariableQuery;
  onChange: (val: PinotVariableQuery) => void;
}) {
  return (
    <>
      {selectTable}
      <div className={'gf-form'} data-testid="sql-editor">
        <SqlEditor
          current={variableQuery.pinotQlCode}
          onChange={(pinotQlCode) => onChange({ ...variableQuery, pinotQlCode })}
        />
      </div>
    </>
  );
}
