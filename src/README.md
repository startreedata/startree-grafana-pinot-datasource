# Pinot

## Version Compatibility

This plugin is compatible with Grafana version 9.1.1 and higher.

## Configuration

You must provide the Controller URL and Broker URL of the Pinot instance.

Authentication tokens supported. StarTree customers
can [visit this link](https://dev.startree.ai/docs/query-data/use-apis-and-build-apps/generate-an-api-token) to generate
a new Pinot token for their cluster.

## Query

### Builder

The builder helps you build a time series query for Pinot. The Table, Time Column, and Metric Column fields are
required.
The query results are formated for the time series panel in Grafana.

### Code Editor

The code editor allows you to run arbitrary sql and will format the Pinot results as either a table or time series.

#### Table mode

The table display mode formats the Pinot response as a table. If you specify the time column alias and format, the time
column will be converted into a Grafana time column.

#### Time Series mode

The time series display mode will automatically format the Pinot response for time series panels. You must specify the
time
column alias, time column format, and metric column alias. The `$__timeGroup()` macro converts the time column
into `1:MILLISECONDS:EPOCH` format.

### Macros

To simplify syntax and to allow for dynamic parts, like date range filters, the query can contain the following macros:

| Name                          | Description                                                                                                                                                                                                                                                                            | Example Output                                                                     |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| $__tableName                  | Replaced by the quoted name of the table                                                                                                                                                                                                                                               | "my_table"                                                                         |
| $__timeGroup(dateTimeColumn)  | Replaced by a expression usable in GROUP BY. The expression uses DATETIMECONVERT to convert the given time column into buckets matching the granularity specified in the query options. The resulting bucket is the time group in milliseconds. The column must be a date time column. | DATETIMECONVERT("ts", '1:SECONDS:TIMESTAMP', '1:MILLISECONDS:EPOCH', '15:SECONDS') |
| $__timeFilter(dateTimeColumn) | Replaced by a time range filter for the specified date time column. The time range matches the format of the column. The specified column must be a date time column.                                                                                                                  | "ts" >= 1721636272 AND "ts" <= 1721657872                                          |
| $__timeFilterMillis(column)   | Replaced by a time range filter in milliseconds for the specified column. The column does not have to be a date time column.                                                                                                                                                           | "ts" >= 1721636272002 AND "ts" <= 1721657872002                                    |
| $__timeTo(dateTimeColumn)     | Replaced by the start time of the currently active time selection. The output matches the format of the specified column. The specified column must be a date time column.                                                                                                             | 1721636272                                                                         |
| $__timeToMillis               | Replaced by the start time in milliseconds of the currently active time selection.                                                                                                                                                                                                     | 1721636272002                                                                      |
| $__timeFrom(dateTimeColumn)   | Replaced by the end time of the currently active time selection. The output matches the format of the specified column. The specified column must be a date time column.                                                                                                               | 1721657872                                                                         |
| $__timeFromMillis             | Replaced by the end time in milliseconds of the currently active time selection.                                                                                                                                                                                                       | 1721657872002                                                                      |
| $__metricAlias                | Replaced by the quoted metric alias specified in the query editor.                                                                                                                                                                                                                     | "my_metric"                                                                        |
| $__timeAlias                  | Replaced by the quoted time alias specified in the query editor.                                                                                                                                                                                                                       | "my_time"                                                                          |

<!-- To help maximize the impact of your README and improve usability for users, we propose the following loose structure:

**BEFORE YOU BEGIN**
- Ensure all links are absolute URLs so that they will work when the README is displayed within Grafana and Grafana.com
- Be inspired ✨ 
  - [grafana-polystat-panel](https://github.com/grafana/grafana-polystat-panel)
  - [volkovlabs-variable-panel](https://github.com/volkovlabs/volkovlabs-variable-panel)

**ADD SOME BADGES**

Badges convey useful information at a glance for users whether in the Catalog or viewing the source code. You can use the generator on [Shields.io](https://shields.io/badges/dynamic-json-badge) together with the Grafana.com API 
to create dynamic badges that update automatically when you publish a new version to the marketplace.

- For the logo field use 'grafana'.
- Examples (label: query)
  - Downloads: $.downloads
  - Catalog Version: $.version
  - Grafana Dependency: $.grafanaDependency
  - Signature Type: $.versionSignatureType

Full example: ![Dynamic JSON Badge](https://img.shields.io/badge/dynamic/json?logo=grafana&query=$.version&url=https://grafana.com/api/plugins/grafana-polystat-panel&label=Marketplace&prefix=v&color=F47A20)

Consider other [badges](https://shields.io/badges) as you feel appropriate for your project.

## Overview / Introduction
Provide one or more paragraphs as an introduction to your plugin to help users understand why they should use it.  

Consider including screenshots:
- in [plugin.json](https://grafana.com/docs/grafana/latest/developers/plugins/metadata/#info) include them as relative links.
- in the README ensure they are absolute URLs.

## Requirements
List any requirements or dependencies they may need to run the plugin.

## Getting Started
Provide a quick start on how to configure and use the plugin.

## Documentation
If your project has dedicated documentation available for users, provide links here. For help in following Grafana's style recommendations for technical documentation, refer to our [Writer's Toolkit](https://grafana.com/docs/writers-toolkit/).

## Contributing
Do you want folks to contribute to the plugin or provide feedback through specific means? If so, tell them how!
-->
