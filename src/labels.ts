export default {
  components: {
    ConfigEditor: {
      dataSourceName: 'Pinot',
      docsLinks: '#',
      controllerUrl: {
        label: 'Controller URL',
        placeholder: 'Controller URL',
      },
      brokerUrl: {
        label: 'Broker URL',
        placeholder: 'Broker URL',
      },
      token: {
        help: 'https://dev.startree.ai/docs/query-data/use-apis-and-build-apps/generate-an-api-token',
        typeLabel: 'Type',
        valueLabel: 'Token',
        valuePlaceholder: 'Token',
      },
      database: {
        label: 'Database',
        placeholder: 'default',
        tooltip: 'Optionally specify the database.',
      },
    },
    QueryEditor: {
      editorType: {
        tooltip: 'Select the query type.',
        label: 'Query Type',
      },
      limit: {
        label: 'Limit',
        tooltip: 'Query limit. Defaults to 1,000,000.',
      },
      metricAlias: {
        tooltip: 'The name of the metric column in the query result. Required for time series display.',
        label: 'Metric Alias',
        placeholder: 'metric',
      },
      timeAlias: {
        tooltip: 'The name of the time column in the query result. The time column should be in 1:MILLISECONDS:EPOCH format. Required for time series display.',
        label: 'Time Alias',
      },
      timeFormat: {
        tooltip: 'The time format of the query result. Required for date time conversions.',
        label: 'Time Format',
      },
      granularity: {
        tooltip: 'Select the granularity of the aggregation. Defaults to the value in query options.',
        label: 'Granularity',
      },
      aggregation: {
        tooltip: 'Select the aggregation function.',
        label: 'Aggregation',
      },
      filters: {
        tooltip: 'Add query filters.',
        label: 'Filters',
      },
      groupBy: {
        tooltip: 'Select group by columns.',
        label: 'Group By',
      },
      orderBy: {
        tooltip: 'Select order by columns.',
        label: 'Order By',
      },
      metricColumn: {
        tooltip: 'Select the metric column. Required.',
        label: 'Metric Column',
      },
      database: {
        tooltip: 'Select the Pinot database. Required.',
        label: 'Database',
      },
      table: {
        tooltip: 'Select the table. Required.',
        label: 'Table',
      },
      timeColumn: {
        tooltip: 'Select the time column for this query. Required.',
        label: 'Time Column',
      },
      sqlEditor: {
        tooltip: 'Sql Editor',
        label: 'Pinot Query',
      },
      sqlPreview: {
        tooltip: 'Preview of the generated sql sent to Pinot.',
        label: 'Sql Preview',
        copyTooltip: 'Copy SQL to clipboard.',
        copiedTooltip: 'Copied!',
      },
      display: {
        tooltip: 'Choose display type.',
        label: 'Display',
      },
      queryOptions: {
        help: 'https://docs.pinot.apache.org/users/user-guide-query/query-options',
        tooltip: 'Add query options.',
        label: 'Query Options',
      },
    },
  },
};
