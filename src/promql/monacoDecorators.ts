import { Monaco } from '@monaco-editor/react';
import { language, languageConfiguration } from './promql';
import { promLanguageDefinition } from './promql.contribution';
import { languages } from 'monaco-editor';
import CompletionItemProvider = languages.CompletionItemProvider;

const languageId = promLanguageDefinition.id;

export function addCompletionItemProvider(monaco: Monaco, provider: CompletionItemProvider): () => void {
  const { dispose } = monaco.languages.registerCompletionItemProvider(languageId, provider);
  return dispose;
}

export function addLanguageConfiguration(monaco: Monaco): () => void {
  const { dispose } = monaco.languages.setLanguageConfiguration(languageId, languageConfiguration);
  return dispose;
}

export function addTokensProvider(monaco: Monaco): () => void {
  const { dispose } = monaco.languages.setMonarchTokensProvider(languageId, language);
  return dispose;
}
