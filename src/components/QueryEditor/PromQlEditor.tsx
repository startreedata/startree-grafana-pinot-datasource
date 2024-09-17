import React, { useEffect } from 'react';
import { SelectTable } from './SelectTable';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { FormLabel } from './FormLabel';
import { TextArea } from '@grafana/ui';
import {
  listTimeSeriesLabels,
  listTimeSeriesLabelValues,
  listTimeSeriesMetrics,
  useTimeSeriesTables,
} from '../../resources/timeseries';

export function PromQlEditor(props: PinotQueryEditorProps) {
  const tables = useTimeSeriesTables(props.datasource);

  useEffect(() => {
    const timeRange = { from: props.data?.timeRange.from, to: props.data?.timeRange.to };

    listTimeSeriesMetrics(props.datasource, {
      tableName: props.query.tableName,
      timeRange: timeRange,
    }).then((metrics) => console.log({ metrics }));

    listTimeSeriesLabels(props.datasource, {
      tableName: props.query.tableName,
      timeRange: timeRange,
    }).then((labels) => console.log({ labels }));

    listTimeSeriesLabelValues(props.datasource, {
      tableName: props.query.tableName,
      labelName: 'startree_env',
      timeRange: timeRange,
    }).then((labelValues) => console.log({ labelValues }));
  }, [props.datasource, props.query.tableName, props.data?.timeRange.from, props.data?.timeRange.to]);

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
          <TextArea onChange={(event) => props.onChange({ ...props.query, promQlCode: event.currentTarget.value })} />
        </>
      </div>
    </>
  );
}
