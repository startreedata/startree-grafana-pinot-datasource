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
    },
    QueryEditor: {
      editorType: {
        tooltip: 'Select query type',
        label: 'Query Type',
      },
      limit: {
        label: 'Limit',
        tooltip: 'Limit',
      },
      metricAlias: {
        tooltip: 'Metric column alias.',
        label: 'Metric Alias',
        placeholder: 'metric',
      },
      timeAlias: {
        tooltip: 'Time column alias.',
        label: 'Time Alias',
      },
      timeFormat: {
        tooltip: 'Pinot Time format.',
        label: 'Time Format',
      },
      granularity: {
        tooltip: 'Select granularity.',
        label: 'Granularity',
      },
      aggregation: {
        tooltip: 'Select aggregation function',
        label: 'Aggregation',
      },
      filters: {
        tooltip: 'Select group by filters',
        label: 'Filters',
      },
      groupBy: {
        tooltip: 'Select group by columns',
        label: 'Group By',
      },
      metricColumn: {
        tooltip: 'Select metric column',
        label: 'Metric Column',
      },
      database: {
        tooltip: 'Select Pinot database',
        label: 'Database',
      },
      table: {
        tooltip: 'Select Pinot Table',
        label: 'Table',
      },
      timeColumn: {
        tooltip: 'Select time column',
        label: 'Time Column',
      },
      sqlEditor: {
        tooltip: 'Sql Editor',
        label: 'Pinot Query',
      },
      sqlPreview: {
        tooltip: 'Sql Preview',
        label: 'Sql Preview',
        copyTooltip: 'Copy SQL to clipboard.',
        copiedTooltip: 'Copied!',
      },
    },
  },
};
