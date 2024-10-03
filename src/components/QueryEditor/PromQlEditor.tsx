import React from 'react';
import { SelectTable } from './SelectTable';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { FormLabel } from './FormLabel';
import { useTimeSeriesTables } from '../../resources/timeseries';
import { InputMetricLegend } from './InputMetricLegend';
import { useCompletionDataProvider } from '../../promql/completionDataProvider';
import { PromQlQueryField } from './PromQlQueryField';

export function PromQlEditor(props: PinotQueryEditorProps) {
  const tables = useTimeSeriesTables(props.datasource);

  const timeRange = {
    to: props.range?.to,
    from: props.range?.from,
  };
  const dataProvider = useCompletionDataProvider(props.datasource, props.query.tableName, timeRange);

  return (
    <>
      <div className={'gf-form'}>
        <SelectTable
          selected={props.query.tableName}
          options={tables}
          onChange={(tableName) => props.onChange({ ...props.query, tableName })}
        />
      </div>
      <div className={'gf-form'}>
        <>
          <FormLabel tooltip={'Query'} label={'Query'} />
          <div style={{ flex: '1 1 auto', height: 50 }}>
            <PromQlQueryField
              dataProvider={dataProvider}
              content={props.query.promQlCode}
              options={{
                codeLens: false,
                lineNumbers: 'off',
                minimap: { enabled: false },
                scrollBeyondLastLine: false,
                automaticLayout: true,
                find: { addExtraSpaceOnTop: false },
                hover: { above: false },
                padding: {
                  top: 6,
                },
                renderLineHighlight: 'none',
              }}
              onChange={(promQlCode) => props.onChange({ ...props.query, promQlCode })}
              onRunQuery={props.onRunQuery}
            />
          </div>
        </>
      </div>
      <div className={'gf-form'}>
        <InputMetricLegend
          current={props.query.legend}
          onChange={(legend) => props.onChange({ ...props.query, legend })}
        />
      </div>
    </>
  );
}
