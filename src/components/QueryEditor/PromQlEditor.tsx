import React, { useCallback, useEffect, useMemo } from 'react';
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
import { Editor, useMonaco } from '@monaco-editor/react';
import { addLanguageConfiguration, addRunQueryShortcut, addTokensProvider } from '../../promql/monaco_decorators';
import { editor } from 'monaco-editor';
import { getCompletionProvider } from '../../promql/completion_provider';
import { MyDataProvider } from '../../promql/completions';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { Label } from '../../promql/situation';
import IStandaloneEditorConstructionOptions = editor.IStandaloneEditorConstructionOptions;

export function PromQlEditor(props: PinotQueryEditorProps) {
  const tables = useTimeSeriesTables(props.datasource);

  const dataProvider = useDataProvider(props.datasource, props.query.tableName, {
    to: props.data?.timeRange.to,
    from: props.data?.timeRange.from,
  });

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
              dataProvider={dataProvider}
              content={props.query.promQlCode}
              onChange={(promQlCode) => props.onChange({ ...props.query, promQlCode })}
              onRunQuery={props.onRunQuery}
            />
          </div>
        </>
      </div>
    </>
  );
}

function useDataProvider(
  datasource: DataSource,
  tableName: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  }
): MyDataProvider {
  const cache = useMemo(
    () => new Map<string, Promise<string[]>>(),
    [datasource, tableName, timeRange.to, timeRange.from]
  );

  const retrieve = (key: string | undefined, generator: () => Promise<string[]>): Promise<string[]> => {
    if (!tableName || !key) {
      return new Promise(() => []);
    }

    if (!cache.has(key)) {
      cache.set(key, generator());
    }
    return cache.get(key) || new Promise(() => []);
  };

  const getMetricNames = () =>
    retrieve('kind=metrics', () =>
      listTimeSeriesMetrics(datasource, {
        tableName: tableName,
        timeRange: { to: timeRange.to, from: timeRange.from },
      })
    );

  const getLabelsFor = (metricName: string | undefined, otherLabels: Label[] | undefined): Promise<string[]> =>
    retrieve(`kind=labels&metric=${metricName}`, () =>
      listTimeSeriesLabels(datasource, {
        tableName: tableName,
        metricName: metricName,
        timeRange: { to: timeRange.to, from: timeRange.from },
      })
    );

  const getLabelValuesFor = (
    metricName: string | undefined,
    labelName: string,
    otherLabels: Label[] | undefined
  ): Promise<string[]> =>
    retrieve(`kind=labelValues&metric=${metricName}&label=${labelName}`, () =>
      listTimeSeriesLabelValues(datasource, {
        tableName: tableName,
        metricName: metricName,
        labelName: labelName,
        timeRange: { to: timeRange.to, from: timeRange.from },
      })
    );

  return {
    getMetricNames,
    getLabelsFor,
    getLabelValuesFor,
  };
}

function MyEditor(props: {
  content: string | undefined;
  dataProvider: MyDataProvider;
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

    if (!monaco.editor.addEditorAction) {
      console.log({ addEditorAction: monaco?.editor?.addEditorAction });
      return;
    }

    //const completionItemsCleanup = addCompletionItems(monaco);
    const languageConfigCleanup = addLanguageConfiguration(monaco);
    const tokensProviderCleanup = addTokensProvider(monaco);
    const runQueryCleanup = addRunQueryShortcut(monaco, props.onRunQuery);

    monaco.languages.registerCompletionItemProvider(
      promLanguageDefinition.id,
      getCompletionProvider(monaco, props.dataProvider)
    );

    return () => {
      languageConfigCleanup();
      tokensProviderCleanup();
      runQueryCleanup();
    };
    //}, [monaco, props.metrics, props.onRunQuery]);
  });

  const options: IStandaloneEditorConstructionOptions = {
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
    theme: 'vs-dark',
  };

  return (
    <Editor
      language={languageId}
      width="100%"
      height="100%"
      value={props.content || ''}
      onChange={props.onChange}
      options={options}
    />
  );
}
