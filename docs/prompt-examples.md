# Prompt Examples

Sample prompts for VAST Admin MCP. Curated highlights also live in the [main README](../README.md#prompt-examples).

## Inventory and capacity

```
List all VAST clusters
List all views on cluster cluster1
Show me all tenants across all clusters
Show me all tenants on cluster cluster1; for each tenant show me the 5 views with the highest used capacity
First, get all available clusters. Then compare views with path "/" across all clusters, showing capacity information
Find all views where logical used capacity is greater than 1TB. For each of these views, get their performance metrics over the last 24 hours and show which views have the highest IOPS
Show me all quotas configured for tenant tenant1 on cluster cluster1
Show me all view policies on cluster cluster1 that support S3
Show me the hardware topology for cluster cluster1
```

## Performance and dataflow

```
Create bandwidth and IOPS graph for cluster1 over the last hour
Create dataflow diagram for cluster1 for /path view on the tenant3 tenant for the last hour
Show me dataflow diagram for 172.21.224.139 on cluster1
Get performance metrics for cnodes on cluster cluster1 over the last 7 days
Get performance metrics for cluster cluster1, then get metrics for all cnodes, and finally get metrics for top 3 views. Show me a summary of IOPS and bandwidth for each object type
```

## Alerts, protection, and support

```
Are there any critical alerts on my clusters that were not acknowledged?
Are there any issues with my configured data protection relationships?
List all snapshots for view path /data/app1 on cluster cluster1 tenant tenant1
Create mini support bundle on cluster1 and name it bundle1. Timeframe should be yesterday at midnight for 4m. Generate it only for cnodes prefixed by cnode-128 and upload it to support without private data.
Find all users prefixed with "s3" on cluster cluster1 tenant tenant1
```

## Read-write (requires `--read-write`)

```
Create a new NFS view on cluster cluster1 with path /data/newview in tenant tenant1
Create a view on cluster cluster1 with path /shared/data in tenant tenant1 that supports both NFS and S3 protocols
Create a snapshot named "backup-2024-01-15" for view path /data/app1 on cluster cluster1, tenant tenant1 and keep it for 24h
Create a clone from snapshot "backup-2024-01-15" of view /data/app1. The clone should be at path /data/app1-clone in tenant tenant1 on cluster cluster1
Set a hard quota of 10TB for view path /data/app1 on cluster cluster1, tenant tenant1
Create 3 new views for VMware based on template.
Create an indestructible snapshot named restore-point_<view name> for all VMware views on cluster1
Refresh a clone from most recent snapshot of view /data/app1 at path /data/app1-clone in tenant tenant1 on cluster cluster1
```
