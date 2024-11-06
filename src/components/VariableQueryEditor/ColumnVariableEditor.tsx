import { SelectColumnType } from './SelectColumnType';
import React from 'react';
import { PinotVariableQuery } from '../../types/PinotVariableQuery';

export function ColumnVariableEditor({
  columns,
  variableQuery,
  sqlPreview,
  selectTable,
  onChange,
}: {
  selectTable: React.JSX.Element;
  variableQuery: PinotVariableQuery;
  sqlPreview: string;
  columns: string[];
  onChange: (val: PinotVariableQuery) => void;
}) {
  return (
    <>
      <div className={'gf-form'} style={{ marginBottom: '0' }}>
        {selectTable}
        <SelectColumnType
          selected={variableQuery.columnType}
          onChange={(columnType) => onChange({ ...variableQuery, columnType })}
        />
      </div>
    </>
  );
}
