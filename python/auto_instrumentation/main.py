# Flask application
from flask import Flask
import boto3
import os
import logging

# Initialize Flask application
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Without instrumentation, the app would not have tracing
@app.route("/aws-sdk-call-auto-instrumentation")
def aws_sdk_call_with_auto_instrumentation():
    # Example Boto3 call to list S3 buckets
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    return {
        "message": "Listed S3 buckets successfully",
        "buckets": [bucket['Name'] for bucket in response['Buckets']]
    }

# Define a simple root endpoint
@app.route("/")
def root_endpoint():
    return "OK"

if __name__ == "__main__":
    port = os.environ.get("PORT", "5000")
    app.run(host="0.0.0.0", port=port, debug=True)