// https://docs.google.com/document/d/19Pb4cV7o7TSc8YViZZEWmjJyHE2gIk6L0hu23WarQQs/edit?tab=t.0#bookmark=id.prvedphv69j6

export const PinotSupportedFunctions = Object.freeze(['rate', 'increase', 'delta', 'irate']);

export const PinotSupportedAggregation = Object.freeze(['sum', 'avg', 'max', 'min', 'count', 'topk', 'bottomk']);

export const PinotSupportedOperators = Object.freeze(['+', '-', '*', '/']);

export const PinotSupportedLabelMatching = Object.freeze(['=', '=~', '!=', '!~']);

export const PinotSupportedDurations = Object.freeze(['y', 'w', 'd', 'h', 'm', 's', 'ms']);

export const PinotSupportedKeywords = Object.freeze([
  ...PinotSupportedFunctions,
  ...PinotSupportedAggregation,
  ...PinotSupportedOperators,
  ...PinotSupportedLabelMatching,
  ...PinotSupportedDurations,
  'offset',
]);
