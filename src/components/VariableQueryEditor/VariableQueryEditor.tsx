import React, { useEffect } from 'react';
import { interpolateVariables, PinotDataQuery } from '../../dataquery/PinotDataQuery';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../../datasource';
import { PinotConnectionConfig } from '../../config/PinotConnectionConfig';
import { SelectVariableType, VariableType } from './SelectVariableType';
import { DistinctValuesVariableEditor } from './DistinctValuesVariableEditor';
import { SqlVariableEditor } from './SqlVariableEditor';
import { ColumnVariableEditor } from './ColumnVariableEditor';
import { VariableQuery } from '../../pinotql';

type VariableQueryEditorProps = QueryEditorProps<DataSource, PinotDataQuery, PinotConnectionConfig, PinotDataQuery>;

export function VariableQueryEditor({ datasource, query, data, onChange: onChangeQuery }: VariableQueryEditorProps) {
  const savedParams = VariableQuery.paramsFrom(query);
  const interpolatedParams = VariableQuery.paramsFrom(interpolateVariables(query, data?.request?.scopedVars));
  const resources = VariableQuery.useResources(datasource, interpolatedParams);
  const onChange = (params: VariableQuery.Params) => onChangeQuery(VariableQuery.dataQueryOf(query, params));

  useEffect(() => {
    if (VariableQuery.applyDefaults(savedParams)) {
      onChange({ ...savedParams });
    }
  });

  return (
    <>
      <div className={'gf-form'} data-testid="select-variable-type">
        <SelectVariableType
          selected={savedParams.variableType}
          onChange={(variableType) => onChange({ ...savedParams, variableType })}
        />
      </div>

      {(() => {
        switch (query.variableQuery?.variableType) {
          case VariableType.ColumnList:
            return <ColumnVariableEditor savedParams={savedParams} resources={resources} onChange={onChange} />;
          case VariableType.DistinctValues:
            return <DistinctValuesVariableEditor savedParams={savedParams} resources={resources} onChange={onChange} />;
          case VariableType.PinotQlCode:
            return <SqlVariableEditor savedParams={savedParams} resources={resources} onChange={onChange} />;
          default:
            return <></>;
        }
      })()}
    </>
  );
}
