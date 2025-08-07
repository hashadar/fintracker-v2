import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    Simple test Lambda function to verify container deployment works properly.
    """
    try:
        # Get environment variable
        environment = os.environ.get('ENVIRONMENT', 'develop')
        
        # Create test response
        test_data = {
            'message': 'Hello from Lambda Container!',
            'environment': environment,
            'timestamp': datetime.now().isoformat(),
            'lambda_request_id': context.request_id,
            'event_data': event
        }
        
        print(f"Test function executed successfully in {environment} environment")
        
        return {
            'statusCode': 200,
            'body': json.dumps(test_data, indent=2)
        }
        
    except Exception as e:
        print(f"Error in test Lambda function: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Test Lambda function failed',
                'timestamp': datetime.now().isoformat()
            }, indent=2)
        } 