import { SelectColumnType } from './SelectColumnType';
import React from 'react';
import { SelectTable } from './SelectTable';
import { VariableParams } from '../../pinotql/variablePararms';
import { VariableResources } from '../../pinotql/variableResources';

export function ColumnVariableEditor(props: {
  savedParams: VariableParams;
  resources: VariableResources;
  onChange: (params: VariableParams) => void;
}) {
  const { savedParams, resources, onChange } = props;
  return (
    <>
      <div className={'gf-form'} style={{ marginBottom: '0' }}>
        <SelectTable
            selected={savedParams.tableName}
            options={resources.tables}
            isLoading={resources.isColumnsLoading}
            onChange={(tableName) => onChange({ ...savedParams, tableName })}
        />
        <SelectColumnType
          selected={savedParams.columnType}
          onChange={(columnType) => onChange({ ...savedParams, columnType })}
        />
      </div>
    </>
  );
}
