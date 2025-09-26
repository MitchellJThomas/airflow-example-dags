# AWS Connection Setup for S3 Operators

## Prerequisites
- AWS credentials (Access Key ID and Secret Access Key)
- S3 bucket named `airflow-example-data` (or update the bucket name in the DAG)
- AWS region configured

## Option 1: Using Airflow UI

1. **Navigate to Admin > Connections**
2. **Click the "+" button to add a new connection**
3. **Configure the connection:**
   - **Connection Id:** `aws_default`
   - **Connection Type:** `Amazon Web Services`
   - **Login:** Your AWS Access Key ID
   - **Password:** Your AWS Secret Access Key
   - **Extra:** `{"region_name": "us-west-2"}` (or your preferred region)

## Option 2: Using Environment Variables

Set these environment variables in your Kubernetes deployment:

```yaml
env:
- name: AWS_ACCESS_KEY_ID
  value: "your-access-key-id"
- name: AWS_SECRET_ACCESS_KEY
  value: "your-secret-access-key"
- name: AWS_DEFAULT_REGION
  value: "us-west-2"
```

## Option 3: Using Kubernetes Secrets

1. **Create a secret:**
```bash
kubectl create secret generic aws-credentials \
  --from-literal=access-key-id=your-access-key-id \
  --from-literal=secret-access-key=your-secret-access-key \
  --namespace=airflow
```

2. **Update your deployment to use the secret:**
```yaml
env:
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: aws-credentials
      key: access-key-id
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: aws-credentials
      key: secret-access-key
- name: AWS_DEFAULT_REGION
  value: "us-west-2"
```

## S3 Bucket Setup

1. **Create the S3 bucket:**
```bash
aws s3 mb s3://airflow-example-data --region us-west-2
```

2. **Set bucket permissions (if needed):**
```bash
aws s3api put-bucket-policy --bucket airflow-example-data --policy '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::YOUR-ACCOUNT-ID:user/YOUR-IAM-USER"
      },
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::airflow-example-data/*"
    }
  ]
}'
```

## Testing the Connection

You can test the connection by running this Python code in your Airflow environment:

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Test the connection
s3_hook = S3Hook(aws_conn_id="aws_default")
buckets = s3_hook.list_buckets()
print("Available buckets:", [bucket['Name'] for bucket in buckets])
```
