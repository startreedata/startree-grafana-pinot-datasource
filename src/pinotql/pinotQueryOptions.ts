// ref https://docs.pinot.apache.org/users/user-guide-query/query-options
// TODO: Is there a pinot api that provides this list?

export const PinotQueryOptions: Array<{ name: string }> = [
  { name: 'timeoutMs' },
  { name: 'enableNullHandling' },
  { name: 'explainPlanVerbose' },
  { name: 'useMultistageEngine' },
  { name: 'maxExecutionThreads' },
  { name: 'numReplicaGroupsToQuery' },
  { name: 'minSegmentGroupTrimSize' },
  { name: 'minServerGroupTrimSize' },
  { name: 'skipIndexes' },
  { name: 'skipUpsert' },
  { name: 'useStarTree' },
  { name: 'maxRowsInJoin' },
  { name: 'inPredicatePreSorted' },
  { name: 'inPredicateLookupAlgorithm' },
  { name: 'maxServerResponseSizeBytes' },
  { name: 'maxQueryResponseSizeBytes' },
];
