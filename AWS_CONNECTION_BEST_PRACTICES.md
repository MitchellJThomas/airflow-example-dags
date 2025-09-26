# AWS Connection Best Practices for Airflow S3 Operations

## Overview
This document outlines the best practices for managing AWS connections in the `example_objectstorage_operators.py` DAG, ensuring secure, configurable, and robust S3 operations.

## Configuration Management

### **1. Environment-Based Configuration**
The DAG uses Airflow Variables for flexible configuration:

```python
# Configurable AWS connection ID
AWS_CONN_ID = Variable.get("aws_conn_id", default_var="aws_default")

# Configurable S3 settings
S3_BUCKET = Variable.get("s3_bucket", default_var="airflow-example-data")
S3_PREFIX = Variable.get("s3_prefix", default_var="air_quality_data/")
```

### **2. Setting Up Airflow Variables**

#### **Via Airflow UI:**
1. Navigate to **Admin > Variables**
2. Add the following variables:

| Variable | Value | Description |
|----------|-------|-------------|
| `aws_conn_id` | `aws_production` | AWS connection ID to use |
| `s3_bucket` | `my-airflow-data-bucket` | S3 bucket name |
| `s3_prefix` | `air_quality/` | S3 key prefix |

#### **Via CLI:**
```bash
# Set AWS connection ID
airflow variables set aws_conn_id aws_production

# Set S3 bucket
airflow variables set s3_bucket my-airflow-data-bucket

# Set S3 prefix
airflow variables set s3_prefix air_quality/
```

#### **Via Environment Variables:**
```bash
export AIRFLOW__VAR__aws_conn_id=aws_production
export AIRFLOW__VAR__s3_bucket=my-airflow-data-bucket
export AIRFLOW__VAR__s3_prefix=air_quality/
```

## AWS Connection Setup

### **1. Connection Types and Best Practices**

#### **Option A: AWS Connection (Recommended)**
```python
# Connection ID: aws_production
# Connection Type: Amazon Web Services
# Login: AKIAIOSFODNN7EXAMPLE
# Password: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
# Extra: {"region_name": "us-west-2"}
```

#### **Option B: S3 Connection**
```python
# Connection ID: s3_production
# Connection Type: S3
# Login: AKIAIOSFODNN7EXAMPLE
# Password: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
# Extra: {"region_name": "us-west-2"}
```

### **2. Security Best Practices**

#### **IAM Roles (Recommended for Production)**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-airflow-data-bucket",
        "arn:aws:s3:::my-airflow-data-bucket/*"
      ]
    }
  ]
}
```

#### **Environment Variables (Development)**
```bash
# Set AWS credentials as environment variables
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-west-2
```

## Connection Validation

### **1. Pre-Flight Checks**
The DAG includes comprehensive connection validation:

```python
@task
def validate_aws_connection():
    """
    Validate AWS connection and bucket access.
    """
    # Test S3Hook initialization
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # Test connection by listing buckets
    buckets = s3_hook.list_buckets()
    
    # Check if target bucket exists
    if S3_BUCKET in bucket_names:
        logger.info(f"✅ Target bucket '{S3_BUCKET}' found")
    else:
        logger.warning(f"⚠️ Target bucket '{S3_BUCKET}' not found")
```

### **2. Error Handling**
```python
try:
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    logger.info(f"🔗 Using AWS connection: {AWS_CONN_ID}")
except Exception as e:
    logger.error(f"❌ Failed to initialize S3Hook: {str(e)}")
    raise AirflowException(f"Cannot initialize S3 connection '{AWS_CONN_ID}': {str(e)}")
```

## Multi-Environment Configuration

### **Development Environment**
```python
# Variables for development
aws_conn_id = "aws_dev"
s3_bucket = "airflow-dev-data"
s3_prefix = "dev/air_quality/"
```

### **Staging Environment**
```python
# Variables for staging
aws_conn_id = "aws_staging"
s3_bucket = "airflow-staging-data"
s3_prefix = "staging/air_quality/"
```

### **Production Environment**
```python
# Variables for production
aws_conn_id = "aws_production"
s3_bucket = "airflow-prod-data"
s3_prefix = "prod/air_quality/"
```

## Connection Testing

### **1. Manual Testing**
```python
# Test connection in Python shell
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Test connection
s3_hook = S3Hook(aws_conn_id="aws_default")
buckets = s3_hook.list_buckets()
print(f"Available buckets: {[b['Name'] for b in buckets]}")
```

### **2. DAG Testing**
```python
# Run the connection validation task
airflow tasks test example_objectstorage_operators validate_aws_connection 2025-01-01
```

## Troubleshooting

### **Common Issues**

#### **1. Connection Not Found**
```
❌ Failed to initialize S3Hook with connection aws_production
```
**Solution**: Create the connection in Airflow UI or check the connection ID.

#### **2. Access Denied**
```
❌ S3 bucket check failed: Access Denied
```
**Solution**: Check IAM permissions and bucket policy.

#### **3. Invalid Credentials**
```
❌ AWS connection validation failed: Invalid credentials
```
**Solution**: Verify AWS credentials and connection configuration.

### **Debugging Steps**

1. **Check Connection Exists**:
   ```bash
   airflow connections list | grep aws
   ```

2. **Test Connection**:
   ```bash
   airflow connections test aws_default
   ```

3. **Verify Variables**:
   ```bash
   airflow variables list | grep -E "(aws_conn_id|s3_bucket|s3_prefix)"
   ```

## Security Considerations

### **1. Credential Management**
- ✅ Use IAM roles when possible
- ✅ Rotate access keys regularly
- ✅ Use least privilege principle
- ❌ Never hardcode credentials in code

### **2. Network Security**
- ✅ Use VPC endpoints for S3
- ✅ Enable S3 bucket encryption
- ✅ Use HTTPS for all S3 operations
- ❌ Avoid public S3 buckets

### **3. Monitoring**
- ✅ Enable CloudTrail for S3 access logging
- ✅ Monitor S3 access patterns
- ✅ Set up alerts for unusual activity
- ✅ Regular security audits

## Best Practices Summary

### **✅ Do's**
- Use Airflow Variables for configuration
- Implement connection validation
- Use descriptive connection names
- Test connections before deployment
- Use IAM roles in production
- Enable comprehensive logging
- Implement proper error handling

### **❌ Don'ts**
- Don't hardcode connection IDs
- Don't use default connection names in production
- Don't store credentials in code
- Don't skip connection validation
- Don't ignore error messages
- Don't use overly permissive IAM policies

## Implementation Checklist

- [ ] Set up Airflow Variables for configuration
- [ ] Create AWS connection with appropriate permissions
- [ ] Test connection validation task
- [ ] Verify S3 bucket access
- [ ] Configure environment-specific settings
- [ ] Set up monitoring and alerting
- [ ] Document connection details
- [ ] Train team on connection management

This configuration ensures secure, flexible, and maintainable AWS S3 operations in your Airflow DAGs.
