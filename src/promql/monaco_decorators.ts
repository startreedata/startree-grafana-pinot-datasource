import { useMonaco } from '@monaco-editor/react';
import { promLanguageDefinition } from 'monaco-promql';
import { language, languageConfiguration } from './promql';
import { languages } from 'monaco-editor';

// Borrowed some of these from startree query console.

const languageId = promLanguageDefinition.id;

export function addLanguageConfiguration(monaco: NonNullable<ReturnType<typeof useMonaco>>): () => void {
  const { dispose } = monaco.languages.setLanguageConfiguration(languageId, languageConfiguration);
  return dispose;
}

export function addTokensProvider(monaco: NonNullable<ReturnType<typeof useMonaco>>): () => void {
  const { dispose } = monaco.languages.setMonarchTokensProvider(languageId, language);
  return dispose;
}

export function addCompletionItems(monaco: NonNullable<ReturnType<typeof useMonaco>>): () => void {
  const disposable = monaco.languages.registerCompletionItemProvider(languageId, {
    provideCompletionItems: (model, position) => {
      const word = model.getWordUntilPosition(position);

      const suggestions = (language.keywords as string[]).map((value) => ({
        label: value,
        kind: monaco.languages.CompletionItemKind.Value,
        insertText: value,
        insertTextRules: languages.CompletionItemInsertTextRule.InsertAsSnippet,
        detail: '',
        range: {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        },
      }));

      return { suggestions };
    },
  });
  return () => disposable.dispose();
}

export function addRunQueryShortcut(
  monaco: NonNullable<ReturnType<typeof useMonaco>>,
  onRunQuery: () => void
): () => void {

  // Register a custom action to run query
  const disposable = monaco.editor.addEditorAction({
    id: 'run-query',
    label: 'Run Query',
    contextMenuOrder: 1,
    contextMenuGroupId: '1_modification',
    keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
    precondition: 'editorLangId == promql', // TODO: Is this necessary?
    run: onRunQuery,
  });

  return () => disposable.dispose();
}

export function addMetricsCompletionBinding(
  monaco: NonNullable<ReturnType<typeof useMonaco>>,
  metrics: string[] | undefined
): () => void {
  const disposable = monaco.languages.registerCompletionItemProvider(languageId, {
    provideCompletionItems: (model, position) => {
      if (!metrics) {
        return { suggestions: [] };
      }

      const word = model.getWordUntilPosition(position);

      const suggestions = metrics.map((metric) => ({
        label: metric,
        kind: monaco.languages.CompletionItemKind.Value,
        insertTextRules: languages.CompletionItemInsertTextRule.InsertAsSnippet,
        insertText: metric,
        range: {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        },
        detail: '',
      }));

      return { suggestions };
    },
  });

  return () => disposable.dispose();
}
