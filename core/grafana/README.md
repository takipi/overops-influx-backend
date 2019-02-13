# Customizing Grafana

In order to make Grafana work as OverOps Dashbaords as well as the oo-as-influx datasource we have created this folder that needs to override Grafana default folder.

When setting up Grafana, choose the approach which best describes your architecture:
* **Standalone** - the overops-as-influx code runs as a standalone fat jar under a dedicated port  

* **Bundle** - the overops-as-influx code runs inside an OverOps API Server (e.g. https://api.overops.com)

* **Proxy** - the Grafana is proxied by the OverOps UI (e.g. https://app.oveorps.com/grafana)

## Installation (assumes no Grafana)
1. Get Grafana:   
`wget https://s3-us-west-2.amazonaws.com/grafana-releases/release/grafana-5.3.4.linux-amd64.tar.gz`
2. Unpack it to `/opt`
3. Change to user: `chown -R ubuntu:ubuntu /opt/grafana-5.3.4`
4. Override `grafana` folder here in the repo onto `/opt/grafana-5.3.4` to override conf provisioning code and public assets.

## Installing plugins
1. Get Boom table plugin:  
`wget https://grafana.com/api/plugins/yesoreyeram-boomtable-panel/versions/0.4.6/download`
2. Unpack it to `<GRAFANA_HOME>/data/plugins`

## Setup Datasource
Based on the chosen architecture, rename one of the templates in `conf/provisioning/datasources` as `conf/provisioning/datasources/oo.yaml` 

## Setup Grafana (when proxying)

1. Edit `/opt/grafana-5.3.4/conf/custom.ini` and make sure the `root_url` points to the correct OverOps server
2. In the provisioning folder, replace `${TAKIPI_HOST_URL}` and `${TAKIPI_API_URL}` in yaml and json files

### Run Grafana
1. `cd /opt/grafana-5.3.4`
2. `nohup bin/grafana-server web &`
