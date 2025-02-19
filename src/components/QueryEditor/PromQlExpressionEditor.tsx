import {
  CompleteStrategy,
  newCompleteStrategy,
  PrometheusClient,
  PromQLExtension,
} from '@prometheus-io/codemirror-promql';
import { Compartment, EditorState, Prec } from '@codemirror/state';
import { EditorView, highlightSpecialChars, keymap, ViewUpdate } from '@codemirror/view';
import { MetricMetadata } from '@prometheus-io/codemirror-promql/dist/cjs/client/prometheus';
import React, { useEffect, useRef, useState } from 'react';
import { Matcher } from '@prometheus-io/codemirror-promql/dist/esm/types';
import { bracketMatching, indentOnInput } from '@codemirror/language';
import {
  autocompletion,
  closeBrackets,
  closeBracketsKeymap,
  CompletionContext,
  completionKeymap,
  CompletionResult,
} from '@codemirror/autocomplete';
import { lintKeymap } from '@codemirror/lint';
import { highlightSelectionMatches } from '@codemirror/search';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { useTheme2 } from '@grafana/ui';
import { DateTime, GrafanaTheme2 } from '@grafana/data';
import { DataSource } from '../../datasource';
import { PinotSupportedKeywords } from '../../promql/pinotSupport';
import { listTimeSeriesLabels, listTimeSeriesLabelValues, listTimeSeriesMetrics } from '../../resources/timeseries';

export function PromQlExpressionEditor(props: {
  datasource: DataSource;
  tableName: string | undefined;
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  };
  value: string | undefined;
  onChange: (v: string) => void;
  onRunQuery: () => void;
}) {
  const { tableName, datasource, timeRange, value, onChange, onRunQuery } = props;

  const grafanaTheme = useTheme2();
  const [dynamicConfigCompartment] = useState(new Compartment());

  const [editorContent, onExpressionChange] = useState(value || '');
  useEffect(() => {
    if (editorContent !== value) {
      onChange(editorContent);
    }
  }, [editorContent, value, onChange]);

  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  useEffect(() => {
    const promClient = getPrometheusClient(datasource, tableName, timeRange);
    const promQL = new PromQLExtension().setComplete({
      completeStrategy: getCompletionStrategy(promClient),
    });

    const dynamicConfig = [promQL.asExtension(), getEditorTheme(grafanaTheme)];

    if (viewRef.current != null) {
      const view = viewRef.current;
      view.dispatch(
        view.state.update({
          effects: dynamicConfigCompartment.reconfigure(dynamicConfig),
        })
      );
      return;
    }

    const view = new EditorView({
      state: EditorState.create({
        doc: editorContent,
        extensions: [
          dynamicConfigCompartment.of(dynamicConfig),
          history(),
          highlightSpecialChars(),
          indentOnInput(),
          bracketMatching(),
          closeBrackets(),
          autocompletion(),
          highlightSelectionMatches(),
          EditorState.allowMultipleSelections.of(true),
          EditorView.lineWrapping,
          keymap.of([...closeBracketsKeymap, ...defaultKeymap, ...historyKeymap, ...completionKeymap, ...lintKeymap]),
          Prec.highest(
            keymap.of([
              {
                key: 'Cmd-Enter',
                run: (v: EditorView): boolean => {
                  onExpressionChange(v.state.doc.toString());
                  onRunQuery();
                  return true;
                },
              },
            ])
          ),
          EditorView.updateListener.of((update: ViewUpdate): void => {
            update.focusChanged && onExpressionChange(update.state.doc.toString());
          }),
        ],
      }),
      parent: containerRef.current || undefined,
    });
    viewRef.current = view;
    view.focus();
  });

  return (
    <div
      style={{
        flex: '1 1 auto',
        border: '1px solid ' + grafanaTheme.colors.border.medium,
        width: '100%',
        padding: '6px',
        background: grafanaTheme.components.input.background,
      }}
      ref={containerRef}
    ></div>
  );
}

// TODO: This wrapper can be removed once we have full Promql language support in Pinot.
function getCompletionStrategy(client: PrometheusClient): CompleteStrategy {
  const base = newCompleteStrategy({ remote: client });
  return {
    async promQL(context: CompletionContext): Promise<CompletionResult | null> {
      return Promise.resolve(base.promQL(context)).then((completionResult) => {
        console.log({ context, completionResult });
        if (completionResult === null) {
          return null;
        }
        completionResult.options = completionResult.options.filter(
          (p) => p.type === 'constant' || p.type === 'text' || PinotSupportedKeywords.includes(p.label)
        );
        return completionResult;
      });
    },
  };
}

export const getEditorTheme = (theme: GrafanaTheme2) =>
  EditorView.theme({
    '&.cm-editor': {
      '&.cm-focused': {
        outline: 'none',
        outline_fallback: 'none',
      },
      background: theme.components.input.background,
    },

    '.cm-content': {
      caretColor: theme.colors.text.primary,
    },

    '.cm-scroller': {
      fontFamily: theme.typography.fontFamily,
    },
    '.cm-placeholder': {
      fontFamily: theme.typography.fontFamily,
    },

    '.cm-matchingBracket': {
      color: theme.colors.text.primary,
      fontWeight: 'bold',
      outline: '1px dashed transparent',
    },

    '.cm-nonmatchingBracket': { borderColor: 'red' },

    '.cm-tooltip': {
      backgroundColor: theme.colors.background.primary,
      borderColor: theme.colors.border.medium,
    },

    '.cm-tooltip.cm-tooltip-autocomplete': {
      '& > ul': {
        maxHeight: '350px',
        fontFamily: theme.typography.fontFamily,
        maxWidth: 'unset',
      },
      '& > ul > li': {
        padding: '2px 1em 2px 3px',
      },
      '& li:hover': {
        backgroundColor: theme.colors.action.hover,
      },
      '& > ul > li[aria-selected]': {
        backgroundColor: theme.colors.action.selected,
        color: 'unset',
      },
      minWidth: '30%',
    },

    '.cm-completionDetail': {
      float: 'right',
      color: theme.colors.text.primary,
    },

    '.cm-tooltip.cm-completionInfo': {
      marginTop: '-11px',
      padding: '10px',
      fontFamily: theme.typography.fontFamily,
      border: 'none',
      backgroundColor: theme.colors.background.secondary,
      minWidth: '250px',
      maxWidth: 'min-content',
    },

    '.cm-completionInfo.cm-completionInfo-right': {
      '&:before': {
        content: "' '",
        height: '0',
        position: 'absolute',
        width: '0',
        left: '-20px',
        border: '10px solid transparent',
        borderRightColor: theme.colors.background.secondary,
      },
      marginLeft: '12px',
    },
    '.cm-completionInfo.cm-completionInfo-left': {
      '&:before': {
        content: "' '",
        height: '0',
        position: 'absolute',
        width: '0',
        right: '-20px',
        border: '10px solid transparent',
        borderLeftColor: theme.colors.background.secondary,
      },
      marginRight: '12px',
    },

    '.cm-completionMatchedText': {
      textDecoration: 'none',
      fontWeight: 'bold',
      color: '#0066bf',
    },

    '.cm-selectionMatch': {
      backgroundColor: '#e6f3ff',
    },

    '.cm-diagnostic': {
      '&.cm-diagnostic-error': {
        borderLeft: '3px solid #e65013',
      },
    },

    '.cm-completionIcon': {
      boxSizing: 'content-box',
      fontSize: '16px',
      lineHeight: '1',
      marginRight: '10px',
      verticalAlign: 'top',
      '&:after': { content: "'\\ea88'" },
      fontFamily: 'codicon',
      paddingRight: '0',
      opacity: '1',
      color: '#007acc',
    },
  });

function getPrometheusClient(
  datasource: DataSource,
  tableName: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  }
): PrometheusClient {
  return {
    metricNames(prefix?: string): Promise<string[]> {
      return listTimeSeriesMetrics(datasource, {
        tableName: tableName,
        timeRange: timeRange,
      });
    },

    labelNames(metricName?: string): Promise<string[]> {
      return listTimeSeriesLabels(datasource, {
        tableName: tableName,
        metricName: metricName,
        timeRange: timeRange,
      });
    },

    labelValues(labelName: string, metricName?: string, matchers?: Matcher[]): Promise<string[]> {
      return listTimeSeriesLabelValues(datasource, {
        tableName: tableName,
        metricName: metricName,
        labelName: labelName,
        timeRange: timeRange,
      });
    },

    // TODO: Implement this handler once metadata api is ready.
    metricMetadata(): Promise<Record<string, MetricMetadata[]>> {
      console.log({ method: 'metricMetadata', args: {} });
      return Promise.resolve({});
    },

    // TODO: Implement this handler once series api is ready.
    series(metricName: string, matchers?: Matcher[], labelName?: string): Promise<Array<Map<string, string>>> {
      console.log({ method: 'series', args: { metricName, matchers, labelName } });
      return Promise.resolve([new Map<string, string>()]);
    },

    // TODO: I actually have no idea what this is used for.
    flags(): Promise<Record<string, string>> {
      console.log({ method: 'flags', args: {} });
      return Promise.resolve({});
    },
  };
}
