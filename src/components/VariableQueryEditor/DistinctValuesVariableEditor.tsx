import React from 'react';
import { SelectColumn } from './SelectColumn';
import { SqlPreview } from './SqlPreview';
import { VariableQuery } from '../../pinotql';

import { SelectTable } from './SelectTable';

export function DistinctValuesVariableEditor(props: {
  savedParams: VariableQuery.Params;
  resources: VariableQuery.Resources;
  onChange: (params: VariableQuery.Params) => void;
}) {
  const { savedParams, resources, onChange } = props;

  return (
    <>
      <div className={'gf-form'} style={{ marginBottom: '0' }}>
        <SelectTable
          selected={savedParams.tableName}
          options={resources.tables}
          isLoading={resources.isTablesLoading}
          onChange={(tableName) => onChange({ ...savedParams, tableName })}
        />
        <SelectColumn
          selected={savedParams.columnName}
          options={resources.columns.filter(({ key }) => !key).map(({ name }) => name)}
          isLoading={resources.isColumnsLoading}
          onChange={(columnName) => onChange({ ...savedParams, columnName })}
        />
      </div>
      <div className={'gf-form'} data-testid="sql-preview">
        <SqlPreview sql={resources.sqlPreview.replace(/\n/g, ' ')} />
      </div>
    </>
  );
}
