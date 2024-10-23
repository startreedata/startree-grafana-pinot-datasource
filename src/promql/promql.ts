import { languages } from 'monaco-editor';
import { PinotSupportedFunctions, PinotSupportedOperators, PinotSupportedVectorMatching } from './pinotSupport';
import IRichLanguageConfiguration = languages.LanguageConfiguration;
import ILanguage = languages.IMonarchLanguage;

export const languageConfiguration: IRichLanguageConfiguration = {
  // the default separators except `@$`
  wordPattern: /(-?\d*\.\d\w*)|([^`~!#%^&*()\-=+\[{\]}\\|;:'",.<>\/?\s]+)/g,
  // Not possible to make comments in PromQL syntax
  comments: {
    lineComment: '#',
  },
  brackets: [
    ['{', '}'],
    ['[', ']'],
    ['(', ')'],
  ],
  autoClosingPairs: [
    { open: '{', close: '}' },
    { open: '[', close: ']' },
    { open: '(', close: ')' },
    { open: '"', close: '"' },
    { open: "'", close: "'" },
  ],
  surroundingPairs: [
    { open: '{', close: '}' },
    { open: '[', close: ']' },
    { open: '(', close: ')' },
    { open: '"', close: '"' },
    { open: "'", close: "'" },
    { open: '<', close: '>' },
  ],
  folding: {},
};

const functions = PinotSupportedFunctions;
const operators = PinotSupportedOperators;
const vectorMatching = ['without', 'by', ...PinotSupportedVectorMatching];
const vectorMatchingRegex = `(${vectorMatching.reduce((prev, curr) => `${prev}|${curr}`)})`;
const offsetModifier = ['offset'];
const keywords = [...functions, ...vectorMatching, ...offsetModifier];

export const language = {
  ignoreCase: false,
  defaultToken: '',
  tokenPostfix: '.promql',

  keywords: keywords,
  operators: operators,
  vectorMatching: vectorMatchingRegex,

  // we include these common regular expressions
  symbols: /[=><!~?:&|+\-*\/^%]+/,
  escapes: /\\(?:[abfnrtv\\"']|x[0-9A-Fa-f]{1,4}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/,
  digits: /\d+(_+\d+)*/,
  octaldigits: /[0-7]+(_+[0-7]+)*/,
  binarydigits: /[0-1]+(_+[0-1]+)*/,
  hexdigits: /[[0-9a-fA-F]+(_+[0-9a-fA-F]+)*/,
  integersuffix: /(ll|LL|u|U|l|L)?(ll|LL|u|U|l|L)?/,
  floatsuffix: /[fFlL]?/,

  // The main tokenizer for our languages
  tokenizer: {
    root: [
      // 'by', 'without' and vector matching
      [/@vectorMatching\s*(?=\()/, 'type', '@clauses'],

      // labels
      [/[a-z_]\w*(?=\s*(=|!=|=~|!~))/, 'tag'],

      // comments
      [/(^#.*$)/, 'comment'],

      // all keywords have the same color
      [
        /[a-zA-Z_]\w*/,
        {
          cases: {
            '@keywords': 'type',
            '@default': 'identifier',
          },
        },
      ],

      // strings
      [/"([^"\\]|\\.)*$/, 'string.invalid'], // non-teminated string
      [/'([^'\\]|\\.)*$/, 'string.invalid'], // non-teminated string
      [/"/, 'string', '@string_double'],
      [/'/, 'string', '@string_single'],
      [/`/, 'string', '@string_backtick'],

      // whitespace
      { include: '@whitespace' },

      // delimiters and operators
      [/[{}()\[\]]/, '@brackets'],
      [/[<>](?!@symbols)/, '@brackets'],
      [
        /@symbols/,
        {
          cases: {
            '@operators': 'delimiter',
            '@default': '',
          },
        },
      ],

      // numbers
      [/\d+[smhdwy]/, 'number'], // 24h, 5m are often encountered in prometheus
      [/\d*\d+[eE]([\-+]?\d+)?(@floatsuffix)/, 'number.float'],
      [/\d*\.\d+([eE][\-+]?\d+)?(@floatsuffix)/, 'number.float'],
      [/0[xX][0-9a-fA-F']*[0-9a-fA-F](@integersuffix)/, 'number.hex'],
      [/0[0-7']*[0-7](@integersuffix)/, 'number.octal'],
      [/0[bB][0-1']*[0-1](@integersuffix)/, 'number.binary'],
      [/\d[\d']*\d(@integersuffix)/, 'number'],
      [/\d(@integersuffix)/, 'number'],
    ],

    string_double: [
      // eslint-disable-line @typescript-eslint/naming-convention
      [/[^\\"]+/, 'string'],
      [/@escapes/, 'string.escape'],
      [/\\./, 'string.escape.invalid'],
      [/"/, 'string', '@pop'],
    ],

    string_single: [
      // eslint-disable-line @typescript-eslint/naming-convention
      [/[^\\']+/, 'string'],
      [/@escapes/, 'string.escape'],
      [/\\./, 'string.escape.invalid'],
      [/'/, 'string', '@pop'],
    ],

    string_backtick: [
      // eslint-disable-line @typescript-eslint/naming-convention
      [/[^\\`$]+/, 'string'],
      [/@escapes/, 'string.escape'],
      [/\\./, 'string.escape.invalid'],
      [/`/, 'string', '@pop'],
    ],

    clauses: [
      [/[^(,)]/, 'tag'],
      [/\)/, 'identifier', '@pop'],
    ],

    whitespace: [[/[ \t\r\n]+/, 'white']],
  },
} as ILanguage;
