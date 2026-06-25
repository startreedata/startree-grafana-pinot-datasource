import type * as monacoTypes from 'monaco-editor';
import {
  ColumnDefinition,
  LanguageDefinition,
  StatementPosition,
  TableDefinition,
  getStandardSQLCompletionProvider,
  grafanaStandardSQLLanguage,
  grafanaStandardSQLLanguageConf,
} from '@grafana/experimental';
import { PINOT_FUNCTIONS, PINOT_MACROS, CLIENT_MACROS, formatPinotSql, lintPinotSql, macroInsertText } from './pinotLanguage';

// Base id for our Monaco language. The SQLEditor registers a per-instance id of `pinotql-<uuid>`,
// which lets us scope the linter to our editors only.
export const PINOT_LANGUAGE_ID = 'pinotql';

const MARKER_OWNER = 'pinotql';

// The monaco namespace handed to a completion provider — `typeof import('monaco-editor')`.
type MonacoArg = Parameters<NonNullable<LanguageDefinition['completionProvider']>>[0];

// Statement positions where the `$__*` macros are offered. Positions not present in Grafana's
// suggestion registry are silently ignored, so listing extras is harmless.
const MACRO_POSITIONS: StatementPosition[] = [
  StatementPosition.SelectKeyword,
  StatementPosition.AfterSelectKeyword,
  StatementPosition.AfterSelectArguments,
  StatementPosition.AfterSelectFuncFirstArgument,
  StatementPosition.SelectAlias,
  StatementPosition.AfterFromKeyword,
  StatementPosition.AfterFrom,
  StatementPosition.WhereKeyword,
  StatementPosition.WhereValue,
  StatementPosition.AfterWhereValue,
  StatementPosition.AfterWhereFunctionArgument,
  StatementPosition.AfterGroupByKeywords,
  StatementPosition.AfterGroupByFunctionArgument,
  StatementPosition.AfterGroupBy,
  StatementPosition.AfterOrderByKeywords,
];

// Monaco instances we've already wired the linter into (one per page in practice).
const validatedMonacos = new WeakSet<object>();

export interface PinotResolvers {
  resolveTables: () => Promise<string[]>;
  resolveColumns: () => Promise<ColumnDefinition[]>;
}

// Builds the LanguageDefinition passed to Grafana's <SQLEditor>. It extends the standard SQL
// completion provider with Pinot tables, columns, functions and `$__*` macros, adds a basic
// "Format query" formatter, and attaches a lightweight syntax linter.
export function buildPinotLanguageDefinition(resolvers: PinotResolvers): LanguageDefinition {
  const completionProvider: LanguageDefinition['completionProvider'] = (monaco, language) => {
    attachLinter(monaco);
    const standard = getStandardSQLCompletionProvider(monaco, language);
    return {
      ...standard,
      supportedFunctions: () => [...(standard.supportedFunctions?.() ?? []), ...PINOT_FUNCTIONS],
      tables: {
        resolve: async (): Promise<TableDefinition[]> =>
          (await resolvers.resolveTables()).map((name) => ({ name, completion: name })),
      },
      columns: {
        resolve: (): Promise<ColumnDefinition[]> => resolvers.resolveColumns(),
      },
      customSuggestionKinds: () => [
        {
          id: 'pinotMacros',
          applyTo: MACRO_POSITIONS,
          suggestionsResolver: async () =>
            [...PINOT_MACROS, ...CLIENT_MACROS].map((macro) => ({
              label: macro.text,
              insertText: macroInsertText(macro),
              insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
              kind: monaco.languages.CompletionItemKind.Snippet,
              sortText: 'a', // surface macros near the top of the list
              detail: 'Pinot macro',
              documentation: macro.description,
            })),
        },
      ],
    };
  };

  return {
    id: PINOT_LANGUAGE_ID,
    loader: () => Promise.resolve({ language: grafanaStandardSQLLanguage, conf: grafanaStandardSQLLanguageConf }),
    completionProvider,
    formatter: formatPinotSql,
  };
}

// Registers a Monaco marker provider that runs lintPinotSql against our editor's models. Scoped by
// the per-instance language id (`pinotql-<uuid>`) so sibling SQL editors are untouched. Idempotent
// per monaco instance.
function attachLinter(monaco: MonacoArg): void {
  if (validatedMonacos.has(monaco)) {
    return;
  }
  validatedMonacos.add(monaco);

  const validate = (model: monacoTypes.editor.ITextModel) => {
    if (!model.getLanguageId().startsWith(PINOT_LANGUAGE_ID)) {
      return;
    }
    const markers: monacoTypes.editor.IMarkerData[] = lintPinotSql(model.getValue()).map((d) => {
      const start = model.getPositionAt(d.start);
      const end = model.getPositionAt(d.end);
      return {
        startLineNumber: start.lineNumber,
        startColumn: start.column,
        endLineNumber: end.lineNumber,
        endColumn: end.column,
        message: d.message,
        severity: monaco.MarkerSeverity.Error,
      };
    });
    monaco.editor.setModelMarkers(model, MARKER_OWNER, markers);
  };

  const watch = (model: monacoTypes.editor.ITextModel) => {
    validate(model);
    model.onDidChangeContent(() => validate(model));
  };

  monaco.editor.getModels().forEach(watch);
  monaco.editor.onDidCreateModel(watch);
}
