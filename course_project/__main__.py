import pulumi
import pulumi_gcp as gcp
import pulumi_docker as docker
import pulumi_kubernetes as k8s
import os
import logging
import base64
import json
import typing

from pulumi_kubernetes.apps.v1 import Deployment, DeploymentSpecArgs
from pulumi_kubernetes.core.v1 import (
    ContainerArgs,
    VolumeArgs,
    VolumeMountArgs,
    SecretVolumeSourceArgs,
    PodSpecArgs,
    PodTemplateSpecArgs,
    EnvVarArgs,
    Secret,
    SecretArgs,
    Namespace,
    NamespaceArgs,
)
from pulumi_kubernetes.meta.v1 import LabelSelectorArgs, ObjectMetaArgs

location = "europe-west1"
gcp_provider = gcp.Provider(
    "gcp_provider",
    region=location,
    zone=f"{location}-b",
    project="streaming-etl-gshpychka",
)

gcp_services_to_enable = ["pubsub", "container", "dataflow", "dataproc", "bigquerydatatransfer"]
gcp_services: dict[str, gcp.projects.Service] = {}

for service in gcp_services_to_enable:
    gcp_services[service] = gcp.projects.Service(
        f"enable-{service}",
        service=f"{service}.googleapis.com",
        opts=pulumi.ResourceOptions(provider=gcp_provider),
        disable_on_destroy=True,
    )

streaming_topic = gcp.pubsub.Topic(
    "incoming_stream",
    opts=pulumi.ResourceOptions(
        provider=gcp_provider, depends_on=gcp_services["pubsub"]
    ),
)

registry = gcp.container.Registry(
    "my-registry",
    opts=pulumi.ResourceOptions(
        provider=gcp_provider, depends_on=gcp_services["container"]
    ),
)

registry_url = registry.id.apply(
    lambda _: gcp.container.get_registry_repository(
        project=typing.cast(str, gcp_provider.project)
    ).repository_url
)
streaming_image_name = registry_url.apply(lambda url: f"{url}/incoming_consumer:v0.0.1")
registry_info = None

images_dir = "containers"
streaming_image = docker.Image(
    "streaming-image",
    build=os.path.join(images_dir, "streaming"),
    image_name=streaming_image_name,
    registry=registry_info,
)

streaming_service_account = gcp.serviceaccount.Account(
    "k8s-account",
    opts=pulumi.ResourceOptions(provider=gcp_provider),
    account_id="gke-pubsub",
    display_name="Service account for GKE with Pub/Sub",
)

publisher_iam = gcp.pubsub.TopicIAMMember(
    "k8s-account-pub",
    role="roles/pubsub.publisher",
    topic=streaming_topic.name,
    member=streaming_service_account.email.apply(lambda x: f"serviceAccount:{x}"),
    opts=pulumi.ResourceOptions(
        provider=gcp_provider, parent=streaming_service_account
    ),
)

service_account_key = gcp.serviceaccount.Key(
    "k8s-account-key",
    service_account_id=streaming_service_account.name,
    public_key_type="TYPE_X509_PEM_FILE",
    opts=pulumi.ResourceOptions(
        provider=gcp_provider,
        parent=streaming_service_account,
        additional_secret_outputs=["privateKey"],
    ),
)


k8s_cluster = gcp.container.Cluster(
    "my-k8s-cluster",
    initial_node_count=1,
    node_config=gcp.container.ClusterNodeConfigArgs(
        preemptible=True,
        machine_type="e2-micro",
        oauth_scopes=[
            "https://www.googleapis.com/auth/compute",
            "https://www.googleapis.com/auth/devstorage.read_only",
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/monitoring",
            # "https://www.googleapis.com/auth/pubsub",
        ],
    ),
    opts=pulumi.ResourceOptions(
        provider=gcp_provider, depends_on=gcp_services["container"]
    ),
)
k8s_info = pulumi.Output.all(
    k8s_cluster.name, k8s_cluster.endpoint, k8s_cluster.master_auth
)
k8s_config = k8s_info.apply(
    lambda info: """apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: {0}
    server: https://{1}
  name: {2}
contexts:
- context:
    cluster: {2}
    user: {2}
  name: {2}
current-context: {2}
kind: Config
preferences: {{}}
users:
- name: {2}
  user:
    auth-provider:
      config:
        cmd-args: config config-helper --format=json
        cmd-path: gcloud
        expiry-key: '{{.credential.token_expiry}}'
        token-key: '{{.credential.access_token}}'
      name: gcp
""".format(
        typing.cast(dict, info[2])["cluster_ca_certificate"],
        info[1],
        "{0}_{1}_{2}".format(gcp_provider.project, gcp_provider.zone, info[0]),
    )
)

k8s_provider = k8s.Provider("gke-k8s", kubeconfig=k8s_config)

labels = {"app": "streaming"}

ns = Namespace(
    "pubsub-ns",
    metadata=dict(name="pubsub", labels=labels),
    opts=pulumi.ResourceOptions(provider=k8s_provider),
)

gcp_creds = Secret(
    "pubsub-creds",
    metadata=dict(namespace=ns.metadata.name, labels=labels),
    kind="Opaque",
    string_data={
        "gcp-credentials.json": service_account_key.private_key.apply(
            lambda x: base64.b64decode(x).decode("utf-8")
        )
    },
    opts=pulumi.ResourceOptions(provider=k8s_provider, parent=ns),
)
deployment = Deployment(
    "streaming-deployment",
    metadata=dict(namespace=ns.metadata.name, labels=labels),
    spec=DeploymentSpecArgs(
        selector=LabelSelectorArgs(match_labels=labels),
        replicas=1,
        template=PodTemplateSpecArgs(
            metadata=ObjectMetaArgs(labels=labels),
            spec=PodSpecArgs(
                volumes=[
                    VolumeArgs(
                        name="google-cloud-key",
                        secret=SecretVolumeSourceArgs(
                            secret_name=gcp_creds.metadata.name
                        ),
                    )
                ],
                containers=[
                    ContainerArgs(
                        name="image",
                        image=streaming_image_name,
                        image_pull_policy="Always",
                        env=[
                            EnvVarArgs(
                                name="PUBSUB_TOPIC_PATH", value=streaming_topic.id
                            ),
                            EnvVarArgs(name="LOG_LEVEL", value=str(logging.INFO)),
                            EnvVarArgs(
                                name="GOOGLE_APPLICATION_CREDENTIALS",
                                value="/var/secrets/google/gcp-credentials.json",
                            ),
                        ],
                        volume_mounts=[
                            VolumeMountArgs(
                                name="google-cloud-key",
                                mount_path="/var/secrets/google",
                            )
                        ],
                    )
                ],
            ),
        ),
    ),
    opts=pulumi.ResourceOptions(provider=k8s_provider),
)


bq_dataset = gcp.bigquery.Dataset(
    "bq-dataset",
    dataset_id="bq_dataset_v1",
    delete_contents_on_destroy=True,
    default_partition_expiration_ms=1000 * 60 * 60 * 24 * 3,
    default_table_expiration_ms=1000 * 60 * 60 * 24 * 3,
    opts=pulumi.ResourceOptions(provider=gcp_provider),
)

parsed_data_schema = [
    dict(name="timestamp", type="TIMESTAMP"),
    dict(name="instrument_name", type="STRING"),
    dict(name="bid", type="NUMERIC"),
    dict(name="ask", type="NUMERIC"),
    dict(name="bid_volume", type="NUMERIC"),
    dict(name="ask_volume", type="NUMERIC"),
]

parsed_input_table = gcp.bigquery.Table(
    "parsed-table",
    dataset_id=bq_dataset.dataset_id,
    table_id="parsed",
    clusterings=["instrument_name"],
    time_partitioning=dict(type="DAY"),
    schema=json.dumps(parsed_data_schema),
    opts=pulumi.ResourceOptions(provider=gcp_provider, delete_before_replace=True),
    deletion_protection=False,
)

udf_bucket = gcp.storage.Bucket(
    "udf-bucket",
    force_destroy=True,
    opts=pulumi.ResourceOptions(provider=gcp_provider),
)

transform_udf_path = os.path.join("extra_src", "transform_events.js")

udf_uploaded = gcp.storage.BucketObject(
    "udf-script",
    bucket=udf_bucket.name,
    source=pulumi.FileAsset(transform_udf_path),
    name="transform.js",
)
udf_script_path = pulumi.Output.all(udf_bucket.id, udf_uploaded.output_name).apply(
    lambda outputs: f"gs://{outputs[0]}/{outputs[1]}"
)

temp_bucket = gcp.storage.Bucket(
    "temp-bucket",
    force_destroy=True,
    opts=pulumi.ResourceOptions(provider=gcp_provider),
)

parsed_table_spec = pulumi.Output.all(
    gcp_provider.project, parsed_input_table.dataset_id, parsed_input_table.table_id
).apply(lambda values: f"{values[0]}:{values[1]}.{values[2]}")

dataflow_service_account = gcp.serviceaccount.Account(
    "dataflow-account",
    account_id="dataflow-pubsub-bq-worker",
    display_name="Service account for Dataflow PubSub_to_BigQuery job",
    opts=pulumi.ResourceOptions(provider=gcp_provider),
)

streaming_subscription = gcp.pubsub.Subscription(
    "incoming_stream_subscription",
    topic=streaming_topic.name,
    opts=pulumi.ResourceOptions(provider=gcp_provider),
)

subscriber_iam = gcp.pubsub.SubscriptionIAMMember(
    "dataflow-pubsub-subscribe-iam",
    role="roles/pubsub.subscriber",
    subscription=streaming_subscription.name,
    member=dataflow_service_account.email.apply(lambda x: f"serviceAccount:{x}"),
    opts=pulumi.ResourceOptions(provider=gcp_provider, parent=dataflow_service_account),
)

pubsub_viewer_iam = gcp.pubsub.SubscriptionIAMMember(
    "dataflow-pubsub-view-iam",
    role="roles/pubsub.viewer",
    subscription=streaming_subscription.name,
    member=dataflow_service_account.email.apply(lambda x: f"serviceAccount:{x}"),
    opts=pulumi.ResourceOptions(provider=gcp_provider, parent=dataflow_service_account),
)

bq_writer_iam = gcp.bigquery.DatasetIamMember(
    "dataflow-bq-write-iam",
    role="roles/bigquery.dataEditor",
    dataset_id=bq_dataset.dataset_id,
    member=dataflow_service_account.email.apply(lambda x: f"serviceAccount:{x}"),
    opts=pulumi.ResourceOptions(provider=gcp_provider, parent=dataflow_service_account),
)

udf_reader_iam = gcp.storage.BucketIAMMember(
    "dataflow-storage-udf-read-iam",
    role="roles/storage.objectViewer",
    bucket=udf_bucket.name,
    member=dataflow_service_account.email.apply(lambda x: f"serviceAccount:{x}"),
    opts=pulumi.ResourceOptions(provider=gcp_provider, parent=dataflow_service_account),
)

temp_reader_iam = gcp.storage.BucketIAMMember(
    "dataflow-storage-temp-read-iam",
    role="roles/storage.objectAdmin",
    bucket=temp_bucket.name,
    member=dataflow_service_account.email.apply(lambda x: f"serviceAccount:{x}"),
    opts=pulumi.ResourceOptions(provider=gcp_provider, parent=dataflow_service_account),
)

dataflow_worker_iam = gcp.projects.IAMMember(
    "dataflow-basic-worker-iam",
    role="roles/dataflow.worker",
    member=dataflow_service_account.email.apply(lambda x: f"serviceAccount:{x}"),
    opts=pulumi.ResourceOptions(provider=gcp_provider, parent=dataflow_service_account),
)

etl_job = gcp.dataflow.Job(
    "pubsub-to-bq",
    template_gcs_path="gs://dataflow-templates/latest/PubSub_Subscription_to_BigQuery",
    temp_gcs_location=temp_bucket.url,
    enable_streaming_engine=True,
    parameters={
        "inputSubscription": streaming_subscription.id,
        "outputTableSpec": parsed_table_spec,
        # "outputDeadletterTable": parsed_table_spec.apply(lambda x: f"{x}_error_records"),
        "javascriptTextTransformGcsPath": udf_script_path,
        "javascriptTextTransformFunctionName": "transform",
    },
    on_delete="cancel",
    max_workers=1,
    opts=pulumi.ResourceOptions(
        provider=gcp_provider, depends_on=gcp_services["dataflow"]
    ),
    service_account_email=dataflow_service_account.email,
)

aggregation_query = """CREATE OR REPLACE VIEW `bq_dataset_v1.aggregated_view`  AS SELECT
TIMESTAMP_TRUNC(timestamp, MINUTE) as timestamp_start,
AVG((ask + bid) / 2) as avg_midprice,
instrument_name
FROM
`streaming-etl-gshpychka.bq_dataset_v1.parsed`
GROUP BY instrument_name, TIMESTAMP_TRUNC(timestamp, MINUTE)
"""
aggregated_view = gcp.bigquery.Job(
    "aggregate-ticks",
    job_id="aggregate-ticks",
    query=gcp.bigquery.JobQueryArgs(query=aggregation_query, allow_large_results=True),
    opts=pulumi.ResourceOptions(
        provider=gcp_provider, depends_on=gcp_services["bigquerydatatransfer"]
    ),
)

dataproc_staging_bucket = gcp.storage.Bucket(
    "dataproc-staging-bucket",
    force_destroy=True,
    opts=pulumi.ResourceOptions(provider=gcp_provider),
)

cluster = gcp.dataproc.Cluster(
    "my-cluster",
    cluster_config=gcp.dataproc.ClusterClusterConfigArgs(
        staging_bucket=dataproc_staging_bucket.name,
        endpoint_config=gcp.dataproc.ClusterClusterConfigEndpointConfigArgs(
            enable_http_port_access=True
        ),
        software_config=gcp.dataproc.ClusterClusterConfigSoftwareConfigArgs(
            image_version="2.0", optional_components=["JUPYTER"]
        ),
        initialization_actions=[
            gcp.dataproc.ClusterClusterConfigInitializationActionArgs(
                script=gcp_provider.region.apply(
                    lambda x: f"gs://goog-dataproc-initialization-actions-{x}/connectors/connectors.sh"
                )
            )
        ],
        gce_cluster_config=gcp.dataproc.ClusterClusterConfigGceClusterConfigArgs(
            metadata={"spark-bigquery-connector-version": "0.21.1"}
        ),
        master_config=gcp.dataproc.ClusterClusterConfigMasterConfigArgs(
            num_instances=1,
            disk_config=gcp.dataproc.ClusterClusterConfigMasterConfigDiskConfigArgs(
                boot_disk_type="pd-ssd",
                boot_disk_size_gb=100,
            ),
        ),
        worker_config=gcp.dataproc.ClusterClusterConfigWorkerConfigArgs(
            num_instances=2,
            disk_config=gcp.dataproc.ClusterClusterConfigWorkerConfigDiskConfigArgs(
                boot_disk_size_gb=30,
                num_local_ssds=1,
            ),
        ),
    ),
    opts=pulumi.ResourceOptions(
        provider=gcp_provider,
        depends_on=gcp_services["dataproc"],
        delete_before_replace=True,
    ),
    region=typing.cast(str, gcp_provider.region),
)

# bucket = gcp.storage.Bucket("input-data", force_destroy=True)

# # Export the DNS name of the bucket
# pulumi.export("cluster_name", cluster.name)
pulumi.export("kubeconfig", k8s_config)
