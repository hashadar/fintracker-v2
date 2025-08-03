FROM public.ecr.aws/lambda/python:3.13

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy your Lambda function code
COPY test_lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the handler
CMD ["test_lambda_function.lambda_handler"] 