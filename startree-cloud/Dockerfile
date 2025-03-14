FROM grafana/grafana:11.5.2

ENV GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=startree-pinot-datasource

COPY ./dist /var/lib/grafana/plugins/startree-pinot-datasource
