# OverOps as Influx

This project goal is to enable Grafana to visualize OverOps (www.overops.com) data and metrics.  

The solution has two components:  
1. A Java REST endpoint that works as an Influx `/query` API.
2. A set of dashboards that uses a special query language in order to fetch OverOps related metrics.

The project is bundled as part of the OverOps API Server , and can be run separately as a Spring boot uber jar.

Follow [grafana/README.MD](grafana/README.MD) for more deployment options and setup instructions.
