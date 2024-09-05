import React from 'react';
import { PinotVariableQuery } from '../../types/PinotVariableQuery';
import { SelectColumn } from './SelectColumn';
import { SqlPreview } from './SqlPreview';

export function DistinctValuesVariableEditor({
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
      <div className={'gf-form'}>
        {selectTable}
        <SelectColumn
          selected={variableQuery?.columnName}
          options={columns}
          onChange={(columnName) => onChange({ ...variableQuery, columnName })}
        />
      </div>
      <div className={'gf-form'}>
        <SqlPreview sql={sqlPreview.replace(/\n/g, ' ')} />
      </div>
    </>
  );
}
