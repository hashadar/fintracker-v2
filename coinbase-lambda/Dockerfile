FROM public.ecr.aws/lambda/python:3.13

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy Lambda function code
COPY daily_data_extractor_lambda.py ${LAMBDA_TASK_ROOT}

# Copy required modules (only coinbase for API access)
COPY coinbase/ ${LAMBDA_TASK_ROOT}/coinbase/

# Set the handler
CMD ["daily_data_extractor_lambda.lambda_handler"] 