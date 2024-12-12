import React from 'react';
import { interpolateVariables, PinotDataQuery } from '../../types/PinotDataQuery';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../../datasource';
import { PinotConnectionConfig } from '../../types/PinotConnectionConfig';
import { SelectVariableType, VariableType } from './SelectVariableType';
import { DistinctValuesVariableEditor } from './DistinctValuesVariableEditor';
import { SqlVariableEditor } from './SqlVariableEditor';
import { ColumnVariableEditor } from './ColumnVariableEditor';
import { dataQueryWithVariableParams, VariableParams, variableParamsFrom } from '../../pinotql/variablePararms';
import { useVariableResources } from '../../pinotql/variableResources';

type VariableQueryEditorProps = QueryEditorProps<DataSource, PinotDataQuery, PinotConnectionConfig, PinotDataQuery>;

export function VariableQueryEditor({ datasource, query, data, onChange: onChangeQuery }: VariableQueryEditorProps) {
  const savedParams = variableParamsFrom(query);
  const interpolatedParams = variableParamsFrom(interpolateVariables(query, data?.request?.scopedVars));
  const resources = useVariableResources(datasource, interpolatedParams);
  const onChange = (params: VariableParams) => onChangeQuery(dataQueryWithVariableParams(query, params));

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
