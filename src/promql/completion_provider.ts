// Borrowed from grafana

import { CompletionType, getCompletions, MyDataProvider } from './completions';
import { getSituation } from './situation';
import { NeverCaseError } from './never_case_error';
import { editor, languages, Position } from 'monaco-editor';
import { Monaco } from '@monaco-editor/react';
import CompletionItemProvider = languages.CompletionItemProvider;
import CompletionList = languages.CompletionList;
import ITextModel = editor.ITextModel;
import ProviderResult = languages.ProviderResult;

function getMonacoCompletionItemKind(type: CompletionType, monaco: Monaco): languages.CompletionItemKind {
  switch (type) {
    case 'DURATION':
      return monaco.languages.CompletionItemKind.Unit;
    case 'FUNCTION':
      return monaco.languages.CompletionItemKind.Variable;
    case 'HISTORY':
      return monaco.languages.CompletionItemKind.Snippet;
    case 'LABEL_NAME':
      return monaco.languages.CompletionItemKind.Enum;
    case 'LABEL_VALUE':
      return monaco.languages.CompletionItemKind.EnumMember;
    case 'METRIC_NAME':
      return monaco.languages.CompletionItemKind.Constructor;
    default:
      throw new NeverCaseError(type);
  }
}

export function getCompletionProvider(monaco: Monaco, dataProvider: MyDataProvider): CompletionItemProvider {
  const provideCompletionItems = (model: ITextModel, position: Position): ProviderResult<CompletionList> => {
    const word = model.getWordAtPosition(position);
    const range =
      word != null
        ? monaco.Range.lift({
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          })
        : monaco.Range.fromPositions(position);

    const positionClone = {
      column: position.column,
      lineNumber: position.lineNumber,
    };

    if (window.getSelection) {
      const selectedText = window.getSelection()?.toString();
      if (selectedText && selectedText.length > 0) {
        positionClone.column = positionClone.column - selectedText.length;
      }
    }

    const offset = model.getOffsetAt(positionClone);
    const situation = getSituation(model.getValue(), offset);

    return getCompletions(situation, dataProvider).then((items) => {
      const maxIndexDigits = items.length.toString().length;
      const suggestions = items.map((item, index) => ({
        kind: getMonacoCompletionItemKind(item.type, monaco),
        label: item.label,
        insertText: item.insertText,
        detail: item.detail,
        documentation: item.documentation,
        sortText: index.toString().padStart(maxIndexDigits, '0'),
        range,
        command: item.triggerOnInsert
          ? {
              id: 'editor.action.triggerSuggest',
              title: '',
            }
          : undefined,
      }));

      return { suggestions };
    });
  };

  return {
    triggerCharacters: ['{', ',', '[', '(', '=', '~', ' ', '"'],
    provideCompletionItems,
  };
}
