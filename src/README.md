# Pinot

## Version Compatibility

This plugin is compatible with Grafana version 9.1.1 and higher.

## Configuration

You must provide the Controller URL and Broker URL of the Pinot instance.

Authentication tokens supported. StarTree customers
can [visit this link](https://docs.startree.ai/corecapabilities/security/manage-api-tokens#generating-an-api-token) to generate
a new Pinot token for their cluster.

### Trace correlation

The datasource settings include two optional correlation sections:

- **Trace to logs** — when the table, trace ID, time, and log message columns are set, each span in a
  trace view gets a "Logs for this trace" link that opens a logs query against the configured logs
  table, filtered by the span's trace ID.
- **Logs to trace** — the reverse direction. When the traces table, trace ID column, and time column
  are set, each log row's trace ID gets a "View trace" link that opens a traces query against the
  configured traces table for that trace ID. The trace ID column must also be added as a metadata
  column on the logs query so its value is present on the row.

## Query

### Builder

The builder helps you build a time series query for Pinot. The Table, Time Column, and Metric Column fields are
required.
The query results are formatted for the time series panel in Grafana.

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

#### Annotations mode

When the datasource is used as an [annotation](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/annotate-visualizations/)
query, Grafana maps the result columns to annotation events by name:

| Column    | Annotation field                                                                 |
|:----------|:---------------------------------------------------------------------------------|
| `time`    | Start time of the annotation (epoch milliseconds — use `$__timeGroup()`).        |
| `timeEnd` | End time. When present, the annotation is rendered as a **region** (start → end). |
| `title`   | Annotation title.                                                                |
| `text`    | Annotation description.                                                          |
| `tags`    | Annotation tags.                                                                 |

Selecting only a `time` column produces point annotations:

```sql
SELECT
  $__timeGroup("event_time") AS "time",
  'My annotation' AS "title",
  'My annotation text' AS "text"
FROM $__table()
WHERE $__timeFilter("event_time")
```

Adding a `timeEnd` column turns each event into a region spanning the start and end times:

```sql
SELECT
  $__timeGroup("start_time") AS "time",
  $__timeGroup("end_time") AS "timeEnd",
  'My annotation' AS "title",
  'My annotation text' AS "text"
FROM $__table()
WHERE $__timeFilter("start_time")
```

### Macros

To simplify syntax and to allow for dynamic parts, like date range filters, the query can contain the following macros:

| Name                                            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Example Output                                                                       |
|:------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------|
| _$\_\_tableName_                                | Replaced by the quoted name of the table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `"my_table"`                                                                         |
| _$\_\_timeGroup(dateTimeColumn, *granularity)_  | Replaced by a expression usable in GROUP BY. The expression uses DATETIMECONVERT to convert the given time column into buckets with of the given granularity. The resulting bucket is the time group in milliseconds. The column must be a date time column. Granularity is optional and defaults to the granularity specified in the panel's query options. If granularity is given, it must be a format usable by DATETIMECONVERT, refer to [DATETIMECONVERT \| Apache Pinot Docs](https://docs.pinot.apache.org/functions/datetime/datetimeconvert). | `DATETIMECONVERT("ts", '1:SECONDS:TIMESTAMP', '1:MILLISECONDS:EPOCH', '15:SECONDS')` |
| _$\_\_timeFilter(dateTimeColumn, *granularity)_ | Replaced by a time range filter for the specified date time column, aligned to the time granularity. The time range matches the format of the column. The specified column must be a date time column. Granularity is optional and defaults to the granularity specified in the panel's query options. If granularity is given, it must be a format usable by DATETIMECONVERT, refer to [DATETIMECONVERT \| Apache Pinot Docs](https://docs.pinot.apache.org/functions/datetime/datetimeconvert).                                                       | `"ts" >= 1721635200 AND "ts" <= 1721656800`                                          |
| _$\_\_timeFilterMillis(column, *granularity)_   | Replaced by a time range filter in milliseconds for the specified column, aligned to the time granularity. The column does not have to be a date time column. Granularity is optional and defaults to the granularity specified in the panel's query options. If granularity is given, it must be a format usable by DATETIMECONVERT, refer to [DATETIMECONVERT \| Apache Pinot Docs](https://docs.pinot.apache.org/functions/datetime/datetimeconvert).                                                                                                | `"ts" >= 1721635200000 AND "ts" <= 1721656800000`                                    |
| _$\_\_timeTo(dateTimeColumn)_                   | Replaced by the exact start time of the currently active time selection. The output matches the format of the specified column. The specified column must be a date time column.                                                                                                                                                                                                                                                                                                                                                                                      | `1721636272`                                                                         |
| _$\_\_timeToMillis_                             | Replaced by the exact start time in milliseconds of the currently active time selection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | `1721636272002`                                                                      |
| _$\_\_timeFrom(dateTimeColumn)_                 | Replaced by the exact end time of the currently active time selection. The output matches the format of the specified column. The specified column must be a date time column.                                                                                                                                                                                                                                                                                                                                                                                        | `1721657872`                                                                         |
| _$\_\_timeFromMillis_                           | Replaced by the exact end time in milliseconds of the currently active time selection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `1721657872002`                                                                      |
| _$\_\_metricAlias_                              | Replaced by the quoted metric alias specified in the query editor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `"my_metric"`                                                                        |
| _$\_\_timeAlias_                                | Replaced by the quoted time alias specified in the query editor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `"my_time"`                                                                          |
| _$\_\_granularityMillis(*granularity)_          | Replaced by the time bucket granularity in milliseconds. Granularity is optional and defaults to the granularity specified in the panel's query options. If granularity is given, it must be a format usable by DATETIMECONVERT, refer to [DATETIMECONVERT \| Apache Pinot Docs](https://docs.pinot.apache.org/functions/datetime/datetimeconvert).                                                                                                                                                                                                     | `3600000`                                                                            |
| _$\_\_interval\_s_                              | Replaced by the time bucket granularity in whole seconds (the seconds counterpart of `$__granularityMillis`). Sub-second granularities truncate to `0`.                                                                                                                                                                                                                                                                                                                                                                                                | `3600`                                                                               |
| _$\_\_panelMillis_                              | Replaced by the total milliseconds in the panel's time selection. Equal to `$__timeToMillis - $__timeFromMillis.`                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `86400000`                                                                           |
| _$\_\_fromTime_                                 | Replaced by the start of the selected time range in epoch **seconds** (the seconds counterpart of `$__timeFromMillis`).                                                                                                                                                                                                                                                                                                                                                                                                                                | `1721635200`                                                                         |
| _$\_\_toTime_                                   | Replaced by the end of the selected time range in epoch **seconds** (the seconds counterpart of `$__timeToMillis`).                                                                                                                                                                                                                                                                                                                                                                                                                                    | `1721656800`                                                                         |
| _$\_\_adHocFilter_                              | Replaced by the dashboard's [ad-hoc filters](https://grafana.com/docs/grafana/latest/dashboards/variables/add-template-variables/#add-ad-hoc-filters) as an AND-joined boolean expression, suitable for a `WHERE` clause. Identifiers are quoted and values escaped. Supported operators: `=`, `!=`, `=~` (regex), `!~` (not regex), `<`, `>`. With no active filters it expands to `TRUE`, so `WHERE $__adHocFilter` is a safe no-op. Applies to PinotQL code queries; place the macro where the filters should go.                                                       | `"city" = 'NY' AND "age" > '30'`                                                     |
| _$\_\_conditionalAll(condition, $variable)_     | Replaced by `condition` unless the [template variable](https://grafana.com/docs/grafana/latest/dashboards/variables/) `$variable` has its **All** option selected (or no value), in which case it expands to `1=1` — dropping the filter. Lets a multi-value variable mean "no filter" when All is picked, instead of generating a huge `IN (...)`. The `condition` may contain commas and parentheses. Applies to PinotQL code queries.                                                                                                                                  | `"service" = '$service'` or `1=1`                                                    |
| _$\_\_searchFilter_                             | For use in **query variable** definitions only. Replaced by what the user types in the variable's dropdown, as a SQL `LIKE` pattern with a trailing `%` (prefix match); expands to `%` (match everything) when nothing is typed. Single quotes are escaped. Write it inside a quoted literal, e.g. `WHERE "name" LIKE '$__searchFilter'`.                                                                                                                                                                                                                | `'check%'` (typed `check`) or `'%'`                                                  |

<br/>

_* Indicates an optional parameter._

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
