locals {
    deployment_prefix = (
        terraform.workspace == "forge-prod" ? "forge-prod-upload-api" :
        terraform.workspace == "forge-test" ? "forge-test-w3infra" :
        "staging-warm-upload-api"
    )

    agent_index_table_name        = "${local.deployment_prefix}-agent-index"
    blob_registry_table_name      = "${local.deployment_prefix}-blob-registry"
    consumer_table_name           = "${local.deployment_prefix}-consumer"
    customer_table_name           = "${local.deployment_prefix}-customer"
    delegation_table_name         = "${local.deployment_prefix}-delegation"
    space_metrics_table_name      = "${local.deployment_prefix}-space-metrics"
    admin_metrics_table_name      = "${local.deployment_prefix}-admin-metrics"
    replica_table_name            = "${local.deployment_prefix}-replica"
    revocation_table_name         = "${local.deployment_prefix}-revocation"
    storage_provider_table_name   = "${local.deployment_prefix}-storage-provider"
    subscription_table_name       = "${local.deployment_prefix}-subscription"
    space_diff_table_name         = "${local.deployment_prefix}-space-diff"
    upload_table_name             = "${local.deployment_prefix}-upload"

    agent_message_bucket_name = (
        terraform.workspace == "forge-prod" ? "forge-prod-upload-api-workflow-store-0" :
        terraform.workspace == "forge-test" ? "workflow-store-forge-test-0" :
        "staging-warm-upload-api-workflow-store-0"
    )
    delegation_bucket_name = (
        terraform.workspace == "forge-prod" ? "forge-prod-upload-api-delegation-0" :
        terraform.workspace == "forge-test" ? "delegation-forge-test-0" :
        "staging-warm-upload-api-delegation-0"
    )
    upload_shards_bucket_name = (
        terraform.workspace == "forge-prod" ? "forge-prod-upload-api-upload-shards-0" :
        terraform.workspace == "forge-test" ? "upload-shards-forge-test-0" :
        "staging-warm-upload-api-upload-shards-0"
    )
}

# Upload service DynamoDB tables
data "aws_dynamodb_table" "agent_index_table" {
  name = local.agent_index_table_name
}

data "aws_dynamodb_table" "blob_registry_table" {
  name = local.blob_registry_table_name
}

data "aws_dynamodb_table" "consumer_table" {
  name = local.consumer_table_name
}

data "aws_dynamodb_table" "customer_table" {
  name = local.customer_table_name
}

data "aws_dynamodb_table" "delegation_table" {
  name = local.delegation_table_name
}

data "aws_dynamodb_table" "space_metrics_table" {
  name = local.space_metrics_table_name
}

data "aws_dynamodb_table" "admin_metrics_table" {
  name = local.admin_metrics_table_name
}

data "aws_dynamodb_table" "replica_table" {
  name = local.replica_table_name
}

data "aws_dynamodb_table" "revocation_table" {
  name = local.revocation_table_name
}

data "aws_dynamodb_table" "storage_provider_table" {
  name = local.storage_provider_table_name
}

data "aws_dynamodb_table" "subscription_table" {
  name = local.subscription_table_name
}

data "aws_dynamodb_table" "space_diff_table" {
  name = local.space_diff_table_name
}

data "aws_dynamodb_table" "upload_table" {
  name = local.upload_table_name
}

# Upload service S3 buckets
data "aws_s3_bucket" "agent_message_bucket" {
  bucket = local.agent_message_bucket_name
}

data "aws_s3_bucket" "delegation_bucket" {
  bucket = local.delegation_bucket_name
}

data "aws_s3_bucket" "upload_shards_bucket" {
  bucket = local.upload_shards_bucket_name
}

# Policies
data "aws_iam_policy_document" "task_upload_service_dynamodb_query_document" {
  statement {
    actions = [
      "dynamodb:Query",
    ]
    resources = [
      data.aws_dynamodb_table.agent_index_table.arn,
      data.aws_dynamodb_table.blob_registry_table.arn,
      data.aws_dynamodb_table.consumer_table.arn,
      data.aws_dynamodb_table.customer_table.arn,
      data.aws_dynamodb_table.delegation_table.arn,
      data.aws_dynamodb_table.space_metrics_table.arn,
      data.aws_dynamodb_table.admin_metrics_table.arn,
      data.aws_dynamodb_table.replica_table.arn,
      data.aws_dynamodb_table.revocation_table.arn,
      data.aws_dynamodb_table.storage_provider_table.arn,
      data.aws_dynamodb_table.subscription_table.arn,
      data.aws_dynamodb_table.space_diff_table.arn,
      data.aws_dynamodb_table.upload_table.arn,
    ]
  }
}

resource "aws_iam_policy" "task_upload_service_dynamodb_query" {
  name        = "${terraform.workspace}-${var.app}-task-upload-service-dynamodb-query"
  description = "This policy will be used by the ECS task to query data from upload-service DynamoDB tables"
  policy      = data.aws_iam_policy_document.task_upload_service_dynamodb_query_document.json
}

resource "aws_iam_role_policy_attachment" "task_upload_service_dynamodb_query" {
  role       = module.app.deployment.task_role.name
  policy_arn = aws_iam_policy.task_upload_service_dynamodb_query.arn
}

data "aws_iam_policy_document" "task_upload_service_s3_get_document" {
  statement {
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "${data.aws_s3_bucket.agent_message_bucket.arn}/*",
      "${data.aws_s3_bucket.delegation_bucket.arn}/*",
      "${data.aws_s3_bucket.upload_shards_bucket.arn}/*",
    ]
  }
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      data.aws_s3_bucket.agent_message_bucket.arn,
      data.aws_s3_bucket.delegation_bucket.arn,
      data.aws_s3_bucket.upload_shards_bucket.arn,
    ]
  }
}

resource "aws_iam_policy" "task_upload_service_s3_get" {
  name        = "${terraform.workspace}-${var.app}-task-upload-service-s3-get"
  description = "This policy will be used by the ECS task to get objects from upload-service S3 buckets"
  policy      = data.aws_iam_policy_document.task_upload_service_s3_get_document.json
}

resource "aws_iam_role_policy_attachment" "task_upload_service_s3_get" {
  role       = module.app.deployment.task_role.name
  policy_arn = aws_iam_policy.task_upload_service_s3_get.arn
}
