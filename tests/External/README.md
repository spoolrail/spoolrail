# External Tests

The External suite verifies the AWS SNS/SQS and Google Pub/Sub drivers against production services. It is intentionally excluded from `composer test`, Git hooks, and routine agent checks because it requires credentials, network access, and billable cloud requests.

Copy the example environment and edit the local file:

```bash
cp .env.external.example .env.external
```

`SPOOLRAIL_PREFIX` marks resources owned by the External suite. Keep the default unless runs may overlap in the same cloud environment; runs sharing a prefix can delete each other's resources during setup or cleanup.

## AWS

Use a dedicated least-privilege IAM user on your AWS account; never use root credentials.

[Create a customer-managed IAM policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_managed-policies.html) and attach it to your test IAM user. In the IAM console, open **Policies → Create policy → JSON**, paste the document below after replacing `REGION` and `ACCOUNT_ID` with their actual values, then save and attach the policy. If `SPOOLRAIL_PREFIX` differs from the default, replace `spoolrail-tests-external` too.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DiscoverExternalTestResources",
            "Effect": "Allow",
            "Action": ["sns:ListTopics", "sqs:ListQueues"],
            "Resource": "*"
        },
        {
            "Sid": "UseExternalTestTopics",
            "Effect": "Allow",
            "Action": [
                "sns:CreateTopic",
                "sns:DeleteTopic",
                "sns:GetSubscriptionAttributes",
                "sns:GetTopicAttributes",
                "sns:ListSubscriptionsByTopic",
                "sns:Publish",
                "sns:Subscribe",
                "sns:Unsubscribe"
            ],
            "Resource": "arn:aws:sns:REGION:ACCOUNT_ID:spoolrail-tests-external-*"
        },
        {
            "Sid": "UseExternalTestQueues",
            "Effect": "Allow",
            "Action": [
                "sqs:CreateQueue",
                "sqs:DeleteMessage",
                "sqs:DeleteQueue",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:ReceiveMessage",
                "sqs:SetQueueAttributes"
            ],
            "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:spoolrail-tests-external-*"
        }
    ]
}
```

> [!NOTE]
> The External policy adds `sns:ListTopics` so cleanup can find test topics left by interrupted runs; the driver itself does not require it.

## Google Pub/Sub

Create a dedicated Google Cloud project, link it to a billing account, and [enable the Cloud Pub/Sub API](https://cloud.google.com/service-usage/docs/enable-disable#enable_a_service). [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create) for the suite and grant it [Pub/Sub Editor](https://cloud.google.com/pubsub/docs/access-control).

[Create a JSON key](https://cloud.google.com/iam/docs/keys-create-delete) for the service account and set `SPOOLRAIL_GOOGLE_CREDENTIALS` to its path.

`SPOOLRAIL_GOOGLE_PUBSUB_ENDPOINT` defaults to `us-east1-pubsub.googleapis.com` because exactly-once delivery is regional. Change it to another [supported locational endpoint](https://cloud.google.com/pubsub/docs/reference/service_apis_overview#list_of_locational_endpoints) if needed.

## Running the Suite

Run the full suite:

```bash
composer test:external
```

Running `composer test:external` without a provider path requires both providers to be configured and accessible.

During a single provider setup or diagnosis, target only that provider:

```bash
composer test:external -- tests/External/SnsSqs
composer test:external -- tests/External/PubSub
```

Requests are billable; see [Amazon SNS pricing](https://aws.amazon.com/sns/pricing/), [Amazon SQS pricing](https://aws.amazon.com/sqs/pricing/), and [Google Pub/Sub pricing](https://cloud.google.com/pubsub/pricing/).

## Finding and Cleaning Up Artifacts

Successful tests request deletion of every resource under `<prefix>-`. AWS deletions can remain visible briefly because SNS removes subscriptions asynchronously and SQS delays reuse of a deleted queue name; the next run uses a new short identifier and refreshes any retained artifacts before testing.

Load the untracked environment into the current shell before using the AWS CLI, then list artifacts with its credentials, Region, and resource prefix:

```bash
set -a
source .env.external
set +a

aws sns list-topics \
    --region "$AWS_DEFAULT_REGION" \
    --query "Topics[?contains(TopicArn, ':${SPOOLRAIL_PREFIX}-')].TopicArn"

aws sqs list-queues \
    --region "$AWS_DEFAULT_REGION" \
    --queue-name-prefix "${SPOOLRAIL_PREFIX}-"
```

Delete retained topics with `aws sns delete-topic --topic-arn ARN` and queues with `aws sqs delete-queue --queue-url URL`. Deleting a suite-owned SNS topic also removes its associated SNS subscription.

List Google Pub/Sub artifacts in the dedicated project:

```bash
gcloud pubsub subscriptions list \
    --project=PROJECT_ID \
    --filter="name:spoolrail-tests-external-"

gcloud pubsub topics list \
    --project=PROJECT_ID \
    --filter="name:spoolrail-tests-external-"
```

Delete retained subscriptions before their topics with `gcloud pubsub subscriptions delete NAME --project=PROJECT_ID` and `gcloud pubsub topics delete NAME --project=PROJECT_ID`.
