# BQMS Python Client

```shell
cd examples/teradata/sql
```

## Local

Provision:

```shell
export BQMS_PROJECT="bqms"
export BQMS_DEVELOPER_EMAIL="dev@google.com"
export BQMS_GCS_BUCKET=$BQMS_PROJECT
export BQMS_GCS_BUCKET_LOCATION="us-east4"

./provision.sh
```

Run:

```shell
export BQMS_VERBOSE="True"
export BQMS_MULTITHREADED="True"
export BQMS_PROJECT="bqms"
export BQMS_GCS_BUCKET=$BQMS_PROJECT

./run.sh
````

Clean/deprovision:

```shell
./clean.sh
./deprovision.sh
```

## Cloud Run

Provision:

```shell
export BQMS_PROJECT="bqms"
export BQMS_DEVELOPER_EMAIL="dev@google.com"
export BQMS_GCS_BUCKET=$BQMS_PROJECT
export BQMS_GCS_BUCKET_LOCATION="us-east4"
export BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME="bqms-sa"
# TODO: The following goes away when we have a public image.
export BQMS_ARTIFACT_PROJECT="bqms-artifact"

./provision.sh
```

Run:

```shell
export BQMS_VERBOSE="True"
export BQMS_MULTITHREADED="True"
export BQMS_PROJECT="bqms"
export BQMS_GCS_BUCKET=$BQMS_PROJECT
export BQMS_CLOUD_RUN_REGION="us-east4"
export BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME="bqms-sa"
export BQMS_CLOUD_RUN_JOB_NAME="bqms"
export BQMS_CLOUD_RUN_ARTIFACT_TAG="us-east4-docker.pkg.dev/bqms-artifact/bqms/bqms:latest"

./run.sh
```

Clean/deprovision:

```shell
./clean.sh
./deprovision.sh
```