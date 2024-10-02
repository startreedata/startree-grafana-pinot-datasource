import React, { useEffect, useMemo, useState } from 'react';
import { SelectTable } from './SelectTable';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { FormLabel } from './FormLabel';
import {
  listTimeSeriesLabels,
  listTimeSeriesLabelValues,
  listTimeSeriesMetrics,
  useTimeSeriesTables,
} from '../../resources/timeseries';
import { promLanguageDefinition } from '../../promql/promql.contribution';
import MonacoEditor, { useMonaco } from '@monaco-editor/react';
import { addLanguageConfiguration, addTokensProvider } from '../../promql/monaco_decorators';
import { languages } from 'monaco-editor';
import { getCompletionProvider } from '../../promql/completion_provider';
import { PromQlCompletionDataProvider } from '../../promql/completions';
import { DataSource } from '../../datasource';
import { DateTime } from '@grafana/data';
import { Label } from '../../promql/situation';
import { InputMetricLegend } from './InputMetricLegend';
import CompletionItemProvider = languages.CompletionItemProvider;

export function PromQlEditor(props: PinotQueryEditorProps) {
  const tables = useTimeSeriesTables(props.datasource);

  const dataProvider = useDataProvider(props.datasource, props.query.tableName, {
    to: props.range?.to,
    from: props.range?.from,
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
            <PromQlQueryField
              dataProvider={dataProvider}
              content={props.query.promQlCode}
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

function useDataProvider(
  datasource: DataSource,
  tableName: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  }
): PromQlCompletionDataProvider {
  const cache = useMemo(
    () => new Map<string, Promise<string[]>>(),
    [datasource, tableName, timeRange.to, timeRange.from] // eslint-disable-line
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

function PromQlQueryField(props: {
  content: string | undefined;
  dataProvider: PromQlCompletionDataProvider;
  onChange: (val: string | undefined) => void;
  onRunQuery: () => void;
}) {
  const [monacoModelId, setMonacoModelId] = useState<string>();
  const monaco = useMonaco();

  const languageId = promLanguageDefinition.id;

  useEffect(() => {
    if (!monaco) {
      return;
    }

    monaco.languages.register(promLanguageDefinition);

    const languageConfigCleanup = addLanguageConfiguration(monaco);
    const tokensProviderCleanup = addTokensProvider(monaco);

    //const runQueryCleanup = addRunQueryShortcut(monaco, props.onRunQuery);

    const completionProvider = getCompletionProvider(monaco, props.dataProvider);
    const filteringCompletionProvider: CompletionItemProvider = {
      ...completionProvider,
      provideCompletionItems: (model, position, context, token) => {
        if (monacoModelId !== model.id) {
          return { suggestions: [] };
        }
        return completionProvider.provideCompletionItems(model, position, context, token);
      },
    };

    const cleanup = monaco.languages.registerCompletionItemProvider(
      promLanguageDefinition.id,
      filteringCompletionProvider
    );

    return () => {
      languageConfigCleanup();
      tokensProviderCleanup();
      // runQueryCleanup();
      cleanup.dispose();
    };
  });

  return (
    <MonacoEditor
      theme={'vs-dark'}
      language={languageId}
      width="100%"
      height="100%"
      value={props.content || ''}
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
      onMount={(editor, monaco) => {
        setMonacoModelId(editor.getModel()?.id);
        editor.addAction({
          id: 'run-query',
          label: 'Run Query',
          contextMenuOrder: 1,
          contextMenuGroupId: '1_modification',
          keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
          run: props.onRunQuery,
        });
      }}
    />
  );
}
