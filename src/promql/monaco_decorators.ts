import { useMonaco } from '@monaco-editor/react';
import { language, languageConfiguration } from './promql';
import { promLanguageDefinition } from './promql.contribution';

const languageId = promLanguageDefinition.id;

export function addLanguageConfiguration(monaco: NonNullable<ReturnType<typeof useMonaco>>): () => void {
  const { dispose } = monaco.languages.setLanguageConfiguration(languageId, languageConfiguration);
  return dispose;
}

export function addTokensProvider(monaco: NonNullable<ReturnType<typeof useMonaco>>): () => void {
  const { dispose } = monaco.languages.setMonarchTokensProvider(languageId, language);
  return dispose;
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
