import { PrometheusClient, PromQLExtension } from '@prometheus-io/codemirror-promql';
import { Compartment, EditorState, Prec } from '@codemirror/state';
import { EditorView, highlightSpecialChars, keymap, ViewUpdate } from '@codemirror/view';
import { MetricMetadata } from '@prometheus-io/codemirror-promql/dist/cjs/client/prometheus';
import React, { useEffect, useRef } from 'react';
import { Matcher } from '@prometheus-io/codemirror-promql/dist/esm/types';
import { bracketMatching, indentOnInput } from '@codemirror/language';
import { autocompletion, closeBrackets, closeBracketsKeymap, completionKeymap } from '@codemirror/autocomplete';
import { lintKeymap } from '@codemirror/lint';
import { highlightSelectionMatches } from '@codemirror/search';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { useTheme2 } from '@grafana/ui';
import { GrafanaTheme2 } from '@grafana/data';
import { DataSource } from '../../datasource'; // Making very good progress on the editor. Needs theming and to remove unsupported functions.

// Making very good progress on the editor. Needs theming and to remove unsupported functions.
// I think that I can just wrap the completion strategy object here https://github.com/prometheus/prometheus/blob/main/web/ui/module/codemirror-promql/src/complete/index.ts
// and filter out the unsupported function suggestions.

const dynamicConfigCompartment = new Compartment();

export function PromQlExpressionEditor(props: {
  datasource: DataSource;
  value: string | undefined;
  onExpressionChange: (v: string) => void;
  onRunQuery: () => void;
}) {
  const theme = useTheme2();

  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);

  const { value, onExpressionChange, onRunQuery } = props;

  useEffect(() => {
    const promQL = new PromQLExtension().setComplete({ remote: promClient });

    const dynamicConfig = [
      promQL.asExtension(),
      // TODO: Add theming?
    ];

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
        doc: value,
        extensions: [
          getEditorTheme(theme),
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
        border: '1px solid ' + theme.colors.border.medium,
        width: '100%',
        padding: '6px',
      }}
      ref={containerRef}
    ></div>
  );
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
      //backgroundColor: theme.colors.background.primary,
      backgroundColor: '#deff0a',
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

    '.cm-line': {
      '&::selection': {
        backgroundColor: theme.components.textHighlight.background,
      },
      '& > span::selection': {
        backgroundColor: theme.components.textHighlight.background,
      },
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

const promClient: PrometheusClient = {
  labelNames(metricName?: string): Promise<string[]> {
    console.log({ method: 'labelNames', args: { metricName } });
    return Promise.resolve(['label1', 'label2']);
  },

  labelValues(labelName: string, metricName?: string, matchers?: Matcher[]): Promise<string[]> {
    console.log({ method: 'labelValues', args: { labelName, metricName, matchers } });
    return Promise.resolve(['val1', 'val2']);
  },

  metricMetadata(): Promise<Record<string, MetricMetadata[]>> {
    console.log({ method: 'metricMetadata', args: {} });
    return Promise.resolve({});
  },

  series(metricName: string, matchers?: Matcher[], labelName?: string): Promise<Map<string, string>[]> {
    console.log({ method: 'series', args: { metricName, matchers, labelName } });
    return Promise.resolve([new Map<string, string>()]);
  },

  metricNames(prefix?: string): Promise<string[]> {
    console.log({ method: 'metricNames', args: { prefix } });
    return Promise.resolve(['metric1', 'metric2']);
  },

  flags(): Promise<Record<string, string>> {
    console.log({ method: 'flags', args: {} });
    return Promise.resolve({});
  },
};
