import React, { useEffect, useState } from 'react';
import { SelectTable } from './SelectTable';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { FormLabel } from './FormLabel';
import {
  listTimeSeriesLabels,
  listTimeSeriesLabelValues,
  listTimeSeriesMetrics,
  useTimeSeriesTables,
} from '../../resources/timeseries';
import { promLanguageDefinition } from 'monaco-promql';
import { useMonaco } from '@monaco-editor/react';
import {
  addCompletionItems,
  addLanguageConfiguration,
  addMetricsCompletionBinding,
  addRunQueryShortcut,
  addTokensProvider,
} from '../../promql/more';
import { ReactMonacoEditor } from '@grafana/ui';
import { } from '@grafana/prometheus'

export function PromQlEditor(props: PinotQueryEditorProps) {
  const tables = useTimeSeriesTables(props.datasource);

  const [metrics, setMetrics] = useState<string[] | undefined>(undefined);

  useEffect(() => {
    // Fetch promql resources for testing purposes.
    // TODO: Remove before merge.

    const timeRange = { from: props.data?.timeRange.from, to: props.data?.timeRange.to };

    listTimeSeriesMetrics(props.datasource, {
      tableName: props.query.tableName,
      timeRange: timeRange,
    }).then(setMetrics);

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
          <div style={{ flex: '1 1 auto', height: 50 }}>
            <MyEditor
              content={props.query.promQlCode}
              metrics={metrics}
              onChange={(promQlCode) => props.onChange({ ...props.query, promQlCode })}
              onRunQuery={props.onRunQuery}
            />
          </div>
        </>
      </div>
    </>
  );
}

function MyEditor(props: {
  content: string | undefined;
  metrics: string[] | undefined;
  onChange: (val: string | undefined) => void;
  onRunQuery: () => void;
}) {
  const monaco = useMonaco();

  const languageId = promLanguageDefinition.id;

  useEffect(() => {
    if (!monaco) {
      return;
    }

    monaco.languages.register(promLanguageDefinition);

    const x = addCompletionItems(monaco);
    const languageConfigCleanup = addLanguageConfiguration(monaco);
    const tokensProviderCleanup = addTokensProvider(monaco);
    const runQueryCleanup = addRunQueryShortcut(monaco, props.onRunQuery);
    const metricsCompletionCleanup = addMetricsCompletionBinding(monaco, props.metrics);

    return () => {
      x();
      languageConfigCleanup();
      tokensProviderCleanup();
      runQueryCleanup();
      metricsCompletionCleanup();
    };
  }, [monaco, props.metrics, props.onRunQuery]);

  return (
    <ReactMonacoEditor
      language={languageId}
      width="100%"
      height="100%"
      value={props.content}
      onChange={props.onChange}
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
    />
  );
}
