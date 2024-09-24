// Borrowed from grafana

import { FUNCTIONS } from './grafana_promql';
import { Label, Situation } from './situation';
import { NeverCaseError } from './never_case_error'; // FIXME: we should not load this from the "outside", but we cannot do that while we have the "old" query-field too

export type CompletionType = 'HISTORY' | 'FUNCTION' | 'METRIC_NAME' | 'DURATION' | 'LABEL_NAME' | 'LABEL_VALUE';

type Completion = {
  type: CompletionType;
  label: string;
  insertText: string;
  detail?: string;
  documentation?: string;
  triggerOnInsert?: boolean;
};

// type Metric = {
//   name: string;
//   help: string;
//   type: string;
// };

export interface MyDataProvider {
  getMetricNames: () => Promise<string[]>;
  getLabelsFor: (metricName: string | undefined, otherLabels?: Label[]) => Promise<string[] | undefined>;
  getLabelValuesFor: (metricName: string | undefined, labelName: string, otherLabels?: Label[]) => Promise<string[]>;
}

async function getAllMetricNamesCompletions(dataProvider: MyDataProvider): Promise<Completion[]> {
  const metrics = await dataProvider.getMetricNames();
  return metrics.map((metric) => ({
    type: 'METRIC_NAME',
    label: metric,
    insertText: metric,
  }));
}

const FUNCTION_COMPLETIONS: Completion[] = FUNCTIONS.map((f) => ({
  type: 'FUNCTION',
  label: f.label,
  insertText: f.insertText ?? '',
  detail: f.detail,
  documentation: f.documentation,
}));

const DURATION_COMPLETIONS: Completion[] = [
  '$__interval',
  '$__range',
  '$__rate_interval',
  '1m',
  '5m',
  '10m',
  '30m',
  '1h',
  '1d',
].map((text) => ({
  type: 'DURATION',
  label: text,
  insertText: text,
}));

async function getLabelNamesForCompletions(
  metric: string | undefined,
  suffix: string,
  triggerOnInsert: boolean,
  otherLabels: Label[],
  dataProvider: MyDataProvider
): Promise<Completion[]> {
  const labels = await dataProvider.getLabelsFor(metric, otherLabels);
  return (labels || []).map((labelName) => ({
    type: 'LABEL_NAME',
    label: labelName,
    insertText: `${labelName}${suffix}`,
    triggerOnInsert,
  }));
}

async function getLabelNamesForSelectorCompletions(
  metric: string | undefined,
  otherLabels: Label[],
  dataProvider: MyDataProvider
): Promise<Completion[]> {
  return getLabelNamesForCompletions(metric, '=', true, otherLabels, dataProvider);
}

async function getLabelNamesForByCompletions(
  metric: string | undefined,
  otherLabels: Label[],
  dataProvider: MyDataProvider
): Promise<Completion[]> {
  return getLabelNamesForCompletions(metric, '', false, otherLabels, dataProvider);
}

async function getLabelValuesForMetricCompletions(
  metric: string | undefined,
  labelName: string,
  betweenQuotes: boolean,
  otherLabels: Label[],
  dataProvider: MyDataProvider
): Promise<Completion[]> {
  return (await dataProvider.getLabelValuesFor(metric, labelName, otherLabels)).map((text) => ({
    type: 'LABEL_VALUE',
    label: text,
    insertText: betweenQuotes ? text : `"${text}"`, // FIXME: escaping strange characters?
  }));
}

export async function getCompletions(
  situation: Situation | null,
  myDataProvider: MyDataProvider
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
      const metricNameCompletions = await getAllMetricNamesCompletions(myDataProvider);
      return [...FUNCTION_COMPLETIONS, ...metricNameCompletions];
    case 'IN_LABEL_SELECTOR_NO_LABEL_NAME':
      return getLabelNamesForSelectorCompletions(situation.metricName, situation.otherLabels, myDataProvider) || [];
    case 'IN_GROUPING':
      return getLabelNamesForByCompletions(situation.metricName, situation.otherLabels, myDataProvider);
    case 'IN_LABEL_SELECTOR_WITH_LABEL_NAME':
      return getLabelValuesForMetricCompletions(
        situation.metricName,
        situation.labelName,
        situation.betweenQuotes,
        situation.otherLabels,
        myDataProvider
      );
    default:
      throw new NeverCaseError(situation);
  }
}
