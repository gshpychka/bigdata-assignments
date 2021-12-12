"""A Google Cloud Python Pulumi program"""

import pulumi
import pulumi_gcp as gcp


provider = gcp.Provider("gcp_provider", region="europe-west1")
cluster = gcp.dataproc.Cluster(
    "my-cluster",
    region=provider.region,
    cluster_config=gcp.dataproc.ClusterClusterConfigArgs(
        master_config=gcp.dataproc.ClusterClusterConfigMasterConfigArgs(
            num_instances=1
        ),
        worker_config=gcp.dataproc.ClusterClusterConfigWorkerConfigArgs(
            num_instances=2
        ),
    ),
)

bucket = gcp.storage.Bucket("input-data", force_destroy=True)


db_instance = gcp.sql.DatabaseInstance(
    "rds",
    region=provider.region,
    database_version="POSTGRES_13",
    deletion_protection=False,
    settings=gcp.sql.DatabaseInstanceSettingsArgs(
        tier="db-f1-micro",
        ip_configuration=gcp.sql.DatabaseInstanceSettingsIpConfigurationArgs(
            authorized_networks=[
                gcp.sql.DatabaseInstanceSettingsIpConfigurationAuthorizedNetworkArgs(
                    value="178.150.229.148/32"
                )
            ],
            ipv4_enabled=True,
            require_ssl=False,
        ),
    ),
)

db = gcp.sql.Database("db", instance=db_instance.name)

# Export the DNS name of the bucket
pulumi.export("cluster_name", cluster.name)
pulumi.export("bucket_name", bucket.name)
pulumi.export("rds_ip", db_instance.public_ip_address)
