import React, { useEffect, useState } from 'react';
import { promLanguageDefinition } from '../../promql/promql.contribution';
import { addCompletionItemProvider, addLanguageConfiguration, addTokensProvider } from '../../promql/monacoDecorators';
import { newCompletionItemProvider, newFilteredCompletionItemProvider } from '../../promql/completionProvider';
import { CompletionDataProvider } from '../../promql/completionDataProvider';
import { editor } from 'monaco-editor';
import { ReactMonacoEditor } from '@grafana/ui';
import { Monaco } from '@monaco-editor/react';

const languageId = promLanguageDefinition.id;

export function PromQlQueryField(props: {
  content: string | undefined;
  dataProvider: CompletionDataProvider;
  onChange: (val: string | undefined) => void;
  onRunQuery: () => void;
  options?: editor.IStandaloneEditorConstructionOptions;
}) {
  const [monaco, setMonaco] = useState<Monaco | undefined>(undefined);
  const [monacoModelId, setMonacoModelId] = useState<string>();

  useEffect(() => {
    if (!monaco) {
      return;
    }

    monaco.languages.register(promLanguageDefinition);

    const languageCleanup = addLanguageConfiguration(monaco);
    const tokensCleanup = addTokensProvider(monaco);
    const completionProvider = newFilteredCompletionItemProvider(
      newCompletionItemProvider(monaco, props.dataProvider),
      monacoModelId
    );
    const completionCleanup = addCompletionItemProvider(monaco, completionProvider);

    return () => {
      languageCleanup();
      tokensCleanup();
      completionCleanup();
    };
  });

  return (
    <ReactMonacoEditor
      language={languageId}
      width="100%"
      height="100%"
      value={props.content || ''}
      onChange={props.onChange}
      options={props.options}
      onMount={(editor, monaco) => {
        setMonaco(monaco);
        setMonacoModelId(editor.getModel()?.id);

        editor.addAction({
          id: 'run-query',
          label: 'Run Query',
          contextMenuOrder: 1,
          contextMenuGroupId: '1_modification',
          keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
          precondition: `editorLangId == ${languageId}`,
          run: props.onRunQuery,
        });
      }}
    />
  );
}
