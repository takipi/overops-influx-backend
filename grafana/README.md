# Customizing Grafana

In order to make Grafana work as OverOps Dashbaords as well as the oo-as-influx datasource we have created this folder that needs to override Grafana default folder.

## Installation
1. Get Grafana: `wget https://s3-us-west-2.amazonaws.com/grafana-releases/release/grafana-5.3.4.linux-amd64.tar.gz`
2. Unpack it to `/opt`
3. Change to user: `chown -R ubuntu:ubuntu /opt/grafana-5.3.4`
4. Override `grafana` folder here onto /opt/grafana-5.3.4

## Setup
1. Edit `/opt/grafana-5.3.4/conf/custom.ini` and make sure the `root_url` points to the correct OverOps server
2. In the provisioning folder, replace `${TAKIPI_HOST_URL}` and `${TAKIPI_API_URL}` in yaml and json files

## Run
1. `cd /opt/grafana-5.3.4`
2. `nohup bin/grafana-server web &`
