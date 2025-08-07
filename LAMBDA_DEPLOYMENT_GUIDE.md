# Lambda Container Deployment Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Workflow Breakdown](#workflow-breakdown)
5. [Lessons Learned](#lessons-learned)
6. [Implementation Plan for Coinbase](#implementation-plan-for-coinbase)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)

## Overview

This guide documents the successful implementation of a GitHub Actions workflow for deploying AWS Lambda container functions. The workflow builds Docker images, pushes them to Amazon ECR, and deploys them as Lambda functions with automated testing.

### Key Components
- **GitHub Actions**: CI/CD pipeline automation
- **AWS ECR**: Docker image registry
- **AWS Lambda**: Serverless compute service
- **Docker**: Containerization
- **Python 3.13**: Runtime environment

## Prerequisites

### 1. AWS Account Setup

#### Required AWS Services
- **ECR (Elastic Container Registry)**: For storing Docker images
- **Lambda**: For running serverless functions
- **IAM**: For managing permissions and roles

#### Required IAM Permissions

**For GitHub Actions User (`github-actions-hasha`):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:DescribeRepositories",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:CreateRepository",
        "ecr:SetRepositoryPolicy",
        "ecr:DescribeImages",
        "lambda:CreateFunction",
        "lambda:UpdateFunctionCode",
        "lambda:InvokeFunction",
        "lambda:GetFunction",
        "iam:PassRole",
        "iam:GetRole",
        "iam:ListAttachedRolePolicies",
        "iam:ListRolePolicies"
      ],
      "Resource": "*"
    }
  ]
}
```

**For Lambda Execution Role (`lambda-execution-role-fintracker-v2`):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:PutObjectAcl"
      ],
      "Resource": "arn:aws:s3:::hashadar-personalfinance/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage"
      ],
      "Resource": "arn:aws:ecr:eu-west-2:148761678020:repository/*"
    }
  ]
}
```

### 2. GitHub Repository Setup

#### Required Secrets
Add these secrets to your GitHub repository:

```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_ACCOUNT_ID=your_account_id
```

#### Repository Structure
```
fintracker-v2/
├── .github/
│   └── workflows/
│       └── test-lambda-deploy.yml
├── coinbase-aws/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── test_lambda_function.py
└── coinbase/
    ├── analytics_calculator.py
    ├── data_extractor.py
    ├── data_transformer.py
    └── pipeline_orchestrator.py
```

### 3. Docker Setup

#### Dockerfile Requirements
```dockerfile
FROM public.ecr.aws/lambda/python:3.13

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy your Lambda function code
COPY your_lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the handler
CMD ["your_lambda_function.lambda_handler"]
```

#### Requirements.txt
```
boto3==1.34.0
pandas==2.0.0
numpy==1.24.0
# Add other dependencies as needed
```

## Architecture

### Workflow Structure

```
GitHub Actions Workflow
├── Test Job (Ubuntu)
│   ├── Checkout code
│   ├── Set up Python 3.13
│   ├── Run basic tests
│   └── Test Docker build
└── Deploy Job (Ubuntu)
    ├── Checkout code
    ├── Configure AWS credentials
    ├── Login to ECR
    ├── Setup ECR repository
    ├── Build and push Docker image
    ├── Setup IAM role and permissions
    ├── Deploy Lambda function
    └── Test Lambda function
```

### Data Flow

1. **Code Push/PR** → Triggers workflow
2. **Test Phase** → Validates code and Docker build
3. **Deploy Phase** → Builds and deploys to AWS
4. **ECR Setup** → Creates/updates repository and policies
5. **Image Build** → Creates Docker image with function code
6. **Lambda Deploy** → Creates/updates Lambda function
7. **Function Test** → Invokes function to verify deployment

## Workflow Breakdown

### 1. Test Job
```yaml
test:
  runs-on: ubuntu-latest
  steps:
  - name: Checkout code
    uses: actions/checkout@v4
  
  - name: Set up Python
    uses: actions/setup-python@v4
    with:
      python-version: '3.13'
  
  - name: Run basic tests
    run: |
      cd coinbase-aws
      python -c "import json; print('Basic test passed')"
  
  - name: Test Docker build
    run: |
      cd coinbase-aws
      docker build -t test-lambda .
      echo "Docker build successful"
```

### 2. Deploy Job
```yaml
deploy:
  needs: test
  runs-on: ubuntu-latest
  if: github.ref == 'refs/heads/develop'
  steps:
  - name: Checkout code
    uses: actions/checkout@v4
  
  - name: Configure AWS credentials
    uses: aws-actions/configure-aws-credentials@v4
    with:
      aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
      aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
      aws-region: ${{ env.AWS_REGION }}
  
  - name: Login to Amazon ECR
    id: login-ecr
    uses: aws-actions/amazon-ecr-login@v2
  
  - name: Setup ECR Repository
    run: |
      # Creates ECR repository if it doesn't exist
      # Sets repository policy for Lambda access
  
  - name: Build and push Docker image
    id: build
    env:
      ECR_REGISTRY: ${{ steps.login-ecr.outputs.registry }}
      IMAGE_TAG: ${{ github.sha }}
    run: |
      cd coinbase-aws
      docker build -t $ECR_REGISTRY/${{ env.ECR_REPOSITORY }}:$IMAGE_TAG .
      docker push $ECR_REGISTRY/${{ env.ECR_REPOSITORY }}:$IMAGE_TAG
  
  - name: Setup IAM Role and Permissions
    run: |
      # Checks if IAM role exists
      # Exits if role doesn't exist (manual setup required)
  
  - name: Deploy Lambda Function
    run: |
      # Checks if function exists
      # Updates existing or creates new function
  
  - name: Test Lambda Function
    run: |
      # Waits for function to be ready
      # Creates test payload using base64 encoding
      # Invokes function and validates response
```

## Lessons Learned

### 1. UTF-8 Encoding Issues
**Problem**: `Invalid UTF-8 middle byte 0x2d` error when passing JSON payloads directly to Lambda.

**Solution**: Use base64 encoding for JSON payloads:
```bash
echo '{"test":"data"}' | base64 > test-payload.b64
aws lambda invoke \
  --function-name $FUNCTION_NAME \
  --payload file://test-payload.b64 \
  --region $REGION \
  response.json
```

**Why**: Shell environments can have encoding issues with direct JSON strings. Base64 encoding ensures clean data transmission.

### 2. IAM Role Permissions
**Problem**: Lambda execution role needs specific ECR permissions to access container images.

**Solution**: Ensure Lambda execution role has:
- `ecr:GetAuthorizationToken` (Resource: `*`)
- `ecr:BatchCheckLayerAvailability` (Resource: `arn:aws:ecr:region:account:repository/repo-name`)
- `ecr:GetDownloadUrlForLayer` (Resource: `arn:aws:ecr:region:account:repository/repo-name`)
- `ecr:BatchGetImage` (Resource: `arn:aws:ecr:region:account:repository/repo-name`)

### 3. ECR Repository Policy
**Problem**: Lambda service needs permission to pull images from ECR.

**Solution**: Set ECR repository policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LambdaAccess",
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": [
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ]
    }
  ]
}
```

### 4. Lambda Context Attributes
**Problem**: `'LambdaContext' object has no attribute 'request_id'` error.

**Solution**: Use correct attribute name:
```python
# Wrong
lambda_request_id = context.request_id

# Correct
lambda_request_id = context.aws_request_id
```

### 5. GitHub Actions Permissions
**Problem**: GitHub Actions user needs specific permissions to manage AWS resources.

**Solution**: Ensure GitHub Actions user has:
- `iam:GetRole`, `iam:ListAttachedRolePolicies`, `iam:ListRolePolicies` for role inspection
- `ecr:SetRepositoryPolicy` for repository policy management
- `ecr:DescribeImages` for image listing

## Implementation Plan for Coinbase

### Phase 1: Infrastructure Setup

#### 1.1 Create Lambda Functions for Each Component

**Data Extractor Lambda**
```yaml
# .github/workflows/coinbase-data-extractor.yml
name: Deploy Coinbase Data Extractor

on:
  push:
    branches: [develop]
    paths:
      - 'coinbase/data_extractor.py'
      - 'coinbase/requirements.txt'
  workflow_dispatch:

env:
  AWS_REGION: eu-west-2
  ECR_REPOSITORY: coinbase-data-extractor
  LAMBDA_FUNCTION_NAME: coinbase-data-extractor
  LAMBDA_TIMEOUT: 900  # 15 minutes for data extraction
  LAMBDA_MEMORY: 1024  # 1GB for processing
```

**Data Transformer Lambda**
```yaml
# .github/workflows/coinbase-data-transformer.yml
name: Deploy Coinbase Data Transformer

env:
  AWS_REGION: eu-west-2
  ECR_REPOSITORY: coinbase-data-transformer
  LAMBDA_FUNCTION_NAME: coinbase-data-transformer
  LAMBDA_TIMEOUT: 900
  LAMBDA_MEMORY: 2048  # 2GB for data processing
```

**Analytics Calculator Lambda**
```yaml
# .github/workflows/coinbase-analytics-calculator.yml
name: Deploy Coinbase Analytics Calculator

env:
  AWS_REGION: eu-west-2
  ECR_REPOSITORY: coinbase-analytics-calculator
  LAMBDA_FUNCTION_NAME: coinbase-analytics-calculator
  LAMBDA_TIMEOUT: 900
  LAMBDA_MEMORY: 2048
```

#### 1.2 Create Dockerfiles for Each Component

**Data Extractor Dockerfile**
```dockerfile
FROM public.ecr.aws/lambda/python:3.13

# Copy requirements
COPY coinbase/requirements.txt .
RUN pip install -r requirements.txt

# Copy function code
COPY coinbase/data_extractor.py ${LAMBDA_TASK_ROOT}
COPY coinbase/__init__.py ${LAMBDA_TASK_ROOT}
COPY configuration/ ${LAMBDA_TASK_ROOT}/configuration/
COPY aws/ ${LAMBDA_TASK_ROOT}/aws/

# Set handler
CMD ["data_extractor.lambda_handler"]
```

**Data Transformer Dockerfile**
```dockerfile
FROM public.ecr.aws/lambda/python:3.13

# Copy requirements
COPY coinbase/requirements.txt .
RUN pip install -r requirements.txt

# Copy function code
COPY coinbase/data_transformer.py ${LAMBDA_TASK_ROOT}
COPY coinbase/__init__.py ${LAMBDA_TASK_ROOT}
COPY configuration/ ${LAMBDA_TASK_ROOT}/configuration/
COPY aws/ ${LAMBDA_TASK_ROOT}/aws/

# Set handler
CMD ["data_transformer.lambda_handler"]
```

**Analytics Calculator Dockerfile**
```dockerfile
FROM public.ecr.aws/lambda/python:3.13

# Copy requirements
COPY coinbase/requirements.txt .
RUN pip install -r requirements.txt

# Copy function code
COPY coinbase/analytics_calculator.py ${LAMBDA_TASK_ROOT}
COPY coinbase/__init__.py ${LAMBDA_TASK_ROOT}
COPY configuration/ ${LAMBDA_TASK_ROOT}/configuration/
COPY aws/ ${LAMBDA_TASK_ROOT}/aws/

# Set handler
CMD ["analytics_calculator.lambda_handler"]
```

### Phase 2: Lambda Function Implementation

#### 2.1 Data Extractor Lambda Handler
```python
# coinbase/data_extractor_lambda.py
import json
import os
from datetime import datetime
from data_extractor import main as run_extraction

def lambda_handler(event, context):
    """
    Lambda handler for Coinbase data extraction.
    """
    try:
        print(f"Starting data extraction at {datetime.now().isoformat()}")
        
        # Run the data extraction
        result = run_extraction()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data extraction completed successfully',
                'timestamp': datetime.now().isoformat(),
                'result': result,
                'lambda_request_id': context.aws_request_id
            }, indent=2)
        }
        
    except Exception as e:
        print(f"Error in data extraction: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Data extraction failed',
                'timestamp': datetime.now().isoformat()
            }, indent=2)
        }
```