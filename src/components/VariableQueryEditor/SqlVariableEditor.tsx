import React from 'react';
import { SqlEditor } from './SqlEditor';
import { SelectTable } from './SelectTable';
import { VariableQuery } from '../../pinotql';

export function SqlVariableEditor(props: {
  savedParams: VariableQuery.Params;
  resources: VariableQuery.Resources;
  onChange: (params: VariableQuery.Params) => void;
}) {
  const { savedParams, resources, onChange } = props;
  return (
    <>
      <SelectTable
        selected={savedParams.tableName}
        options={resources.tables}
        isLoading={resources.isColumnsLoading}
        onChange={(tableName) => onChange({ ...savedParams, tableName })}
      />
      <div className={'gf-form'} data-testid="sql-editor">
        <SqlEditor
          current={savedParams.pinotQlCode}
          onChange={(pinotQlCode) => onChange({ ...savedParams, pinotQlCode })}
        />
      </div>
    </>
  );
}
