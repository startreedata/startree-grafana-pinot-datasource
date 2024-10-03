import { getSituation, Label, Situation } from './situation';
import { editor, languages, Position } from 'monaco-editor';
import { Monaco } from '@monaco-editor/react';
import { Completion, CompletionType } from './completion';
import { CompletionDataProvider } from './completionDataProvider';
import { DURATION_COMPLETIONS } from './durationCompletions';
import { FUNCTION_COMPLETIONS } from './functionCompletions';
import CompletionItemProvider = languages.CompletionItemProvider;
import CompletionList = languages.CompletionList;
import ITextModel = editor.ITextModel;
import ProviderResult = languages.ProviderResult;

export function newFilteredCompletionItemProvider(
  completionProvider: CompletionItemProvider,
  modelId: string | undefined
): CompletionItemProvider {
  return {
    ...completionProvider,
    provideCompletionItems: (model, position, context, token) => {
      if (modelId !== model.id) {
        return { suggestions: [] };
      }
      return completionProvider.provideCompletionItems(model, position, context, token);
    },
  };
}

export function newCompletionItemProvider(
  monaco: Monaco,
  dataProvider: CompletionDataProvider
): CompletionItemProvider {
  const provideCompletionItems = (model: ITextModel, position: Position): ProviderResult<CompletionList> => {
    const word = model.getWordAtPosition(position);

    const adjustedPosition = {
      column: position.column,
      lineNumber: position.lineNumber,
    };

    if (window.getSelection) {
      const selectedText = window.getSelection()?.toString();
      if (selectedText && selectedText.length > 0) {
        adjustedPosition.column = adjustedPosition.column - selectedText.length;
      }
    }

    const offset = model.getOffsetAt(adjustedPosition);
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
        range: getRangeFor(word, monaco, position),
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

function getRangeFor(word: editor.IWordAtPosition | null, monaco: Monaco, position: Position) {
  if (word !== null) {
    return monaco.Range.lift({
      startLineNumber: position.lineNumber,
      endLineNumber: position.lineNumber,
      startColumn: word.startColumn,
      endColumn: word.endColumn,
    });
  } else {
    return monaco.Range.fromPositions(position);
  }
}

function getMonacoCompletionItemKind(type: CompletionType, monaco: Monaco): languages.CompletionItemKind {
  switch (type) {
    case CompletionType.DURATION:
      return monaco.languages.CompletionItemKind.Unit;
    case CompletionType.FUNCTION:
      return monaco.languages.CompletionItemKind.Variable;
    case CompletionType.LABEL_NAME:
      return monaco.languages.CompletionItemKind.Enum;
    case CompletionType.LABEL_VALUE:
      return monaco.languages.CompletionItemKind.EnumMember;
    case CompletionType.METRIC_NAME:
      return monaco.languages.CompletionItemKind.Constructor;
    default:
      throw new Error('should never happen');
  }
}

export async function getCompletions(
  situation: Situation | null,
  dataProvider: CompletionDataProvider
): Promise<Completion[]> {
  if (situation === null) {
    return [];
  }

  switch (situation.type) {
    case 'IN_DURATION':
      return DURATION_COMPLETIONS;
    case 'IN_FUNCTION':
    case 'AT_ROOT':
    case 'EMPTY':
      return [...FUNCTION_COMPLETIONS, ...(await listMetricNameCompletions(dataProvider))];
    case 'IN_LABEL_SELECTOR_NO_LABEL_NAME':
      return listSelectorLabelNameCompletions(situation.metricName, situation.otherLabels, dataProvider);
    case 'IN_GROUPING':
      return listByLabelNameCompletions(situation.metricName, situation.otherLabels, dataProvider);
    case 'IN_LABEL_SELECTOR_WITH_LABEL_NAME':
      return listLabelValuesForMetricCompletions(
        situation.metricName,
        situation.labelName,
        situation.betweenQuotes,
        situation.otherLabels,
        dataProvider
      );
    default:
      throw new Error('should never happen');
  }
}

async function listMetricNameCompletions(dataProvider: CompletionDataProvider): Promise<Completion[]> {
  const metrics = await dataProvider.getMetricNames();
  return metrics.map((metric) => ({
    type: CompletionType.METRIC_NAME,
    label: metric,
    insertText: metric,
  }));
}

// completions for `my_metric{...}`
async function listSelectorLabelNameCompletions(
  metric: string | undefined,
  otherLabels: Label[],
  dataProvider: CompletionDataProvider
): Promise<Completion[]> {
  const labels = await dataProvider.getLabelsFor(metric, otherLabels);
  return (labels || []).map((labelName) => ({
    type: CompletionType.LABEL_NAME,
    label: labelName,
    insertText: `${labelName}=`,
    triggerOnInsert: true,
  }));
}

// completions for `sum(my_metric) by(...)`
async function listByLabelNameCompletions(
  metric: string | undefined,
  otherLabels: Label[],
  dataProvider: CompletionDataProvider
): Promise<Completion[]> {
  const labels = await dataProvider.getLabelsFor(metric, otherLabels);
  return (labels || []).map((labelName) => ({
    type: CompletionType.LABEL_NAME,
    label: labelName,
    insertText: labelName,
  }));
}

async function listLabelValuesForMetricCompletions(
  metric: string | undefined,
  labelName: string,
  betweenQuotes: boolean,
  otherLabels: Label[],
  dataProvider: CompletionDataProvider
): Promise<Completion[]> {
  return (await dataProvider.getLabelValuesFor(metric, labelName, otherLabels)).map((text) => ({
    type: CompletionType.LABEL_VALUE,
    label: text,
    insertText: betweenQuotes ? text : `"${text}"`,
  }));
}
