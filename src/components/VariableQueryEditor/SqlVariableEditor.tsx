import React from 'react';
import { SqlEditor } from './SqlEditor';
import { SelectTable } from './SelectTable';
import { VariableParams } from '../../pinotql/variablePararms';
import { VariableResources } from '../../pinotql/variableResources';

export function SqlVariableEditor(props: {
  savedParams: VariableParams;
  resources: VariableResources;
  onChange: (params: VariableParams) => void;
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
