# Coinbase Daily Data Extractor Lambda

Automated daily extraction of Coinbase portfolio data, market information, and hourly price history. Runs at 2am UTC via EventBridge scheduling.

## What It Does

- **Portfolio Summary**: Current positions, balances, values, and P&L
- **Hourly Price Data**: 24-hour OHLCV data for all portfolio assets  
- **Market Data**: Current prices, spreads, volume, and 24h changes
- **S3 Storage**: Organized daily snapshots in `{environment}/coinbase/raw/`

## Data Structure

```
{environment}/coinbase/raw/
├── positions/
│   ├── daily/positions_YYYYMMDD.json
│   └── latest/positions.json
├── prices/
│   ├── hourly/hourly_prices_YYYYMMDD.json
│   └── latest/hourly_prices.json
└── market/
    ├── daily/market_YYYYMMDD.json
    └── latest/market.json
```

## Deployment

### Prerequisites

1. **AWS Resources**
   - ECR repository: `coinbase-daily-extractor`
   - Lambda function: `coinbase-daily-data-extractor`
   - IAM role: `lambda-execution-role-fintracker-v2`
   - EventBridge rule: `coinbase-daily-extraction-rule`

2. **IAM Permissions** (complete policy for lambda-execution-role-fintracker-v2)
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
       },
       {
         "Effect": "Allow",
         "Action": [
           "secretsmanager:GetSecretValue"
         ],
         "Resource": "arn:aws:secretsmanager:eu-west-2:148761678020:secret:coinbase/api-credentials-*"
       }
     ]
   }
   ```

3. **GitHub Actions IAM Policy** (for deployment automation)
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
           "lambda:CreateFunction",
           "lambda:UpdateFunctionCode",
           "lambda:UpdateFunctionConfiguration",
           "lambda:GetFunctionConfiguration",
           "lambda:InvokeFunction",
           "lambda:GetFunction",
           "lambda:AddPermission",
           "events:PutRule",
           "events:PutTargets",
           "events:DescribeRule",
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

3. **GitHub Secrets**
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY` 
   - `AWS_ACCOUNT_ID`

### Setup

1. **Create Secret in AWS Secrets Manager**
   - Name: `coinbase/api-credentials`
   - Format: JSON with `COINBASE_API_KEY` and `COINBASE_API_SECRET`

2. **Deploy**
   - Push to `develop` branch → deploys to develop environment
   - Push to `main` branch → deploys to production environment

### Environment Variables (Auto-set)
- `ENVIRONMENT`: develop/production (based on branch)
- `S3_BUCKET_NAME`: hashadar-personalfinance

## Monitoring

- **Logs**: `/aws/lambda/coinbase-daily-data-extractor`
- **Metrics**: Invocations, duration, errors, throttles
- **Alarms**: Function errors > 0, duration > 10 minutes

## Testing

```bash
# Test Lambda function
echo '{"test": "data"}' | base64 > test-payload.b64
aws lambda invoke \
  --function-name coinbase-daily-data-extractor \
  --payload file://test-payload.b64 \
  --region eu-west-2 \
  response.json
```

## Troubleshooting

**Common Issues**:
- **Timeout**: Increase Lambda timeout (max 15 minutes)
- **Memory**: Increase allocation if processing large datasets
- **Permissions**: Verify IAM role has Secrets Manager access
- **API Errors**: Check credentials in `coinbase/api-credentials` secret
- **Missing Data**: Ensure `ENVIRONMENT` and `S3_BUCKET_NAME` are set

**Debugging**:
```bash
# Check logs
aws logs tail /aws/lambda/coinbase-daily-data-extractor --follow
```

## Cost Estimate

- **Lambda**: ~$15-30/month (2GB memory, 5-10 min execution)
- **ECR**: ~$1-2/month
- **S3**: ~$1-5/month  
- **EventBridge**: ~$1/month
- **Total**: ~$20-40/month 