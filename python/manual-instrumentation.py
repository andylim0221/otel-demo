from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from flask import Flask
import boto3
import os

# Propogators
from opentelemetry.propagators.aws import AwsXRayPropagator
from opentelemetry.propagators.aws.aws_xray_propagator import (
    TRACE_ID_DELIMITER,
    TRACE_ID_FIRST_PART_LENGTH,
    TRACE_ID_VERSION,
)

# Instrumentation libraries
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

# Initialize Flask application
app = Flask(__name__)

# Set up OpenTelemetry
resource = Resource.create({"service.name": "manual-instrumentation"})

# Set up the tracer provider with the resource
trace_provider = TracerProvider(resource=resource)
metric_provider = MeterProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")))
trace_provider.add_span_processor(processor)
metrics.set_meter_provider(metric_provider)
trace.set_tracer_provider(trace_provider)

tracer = trace.get_tracer(__name__)
metric = metrics.get_meter(__name__)

aws_sdk_call_with_instrumentation_count = metric.create_counter(
    "aws_sdk_call_with_instrumentation_count",
    unit="0",
    description="Count of AWS SDK calls with instrumentation",
)

def setup_instrumentation():
    # Auto-instrument Botocore for AWS SDK
    BotocoreInstrumentor().instrument()
    FlaskInstrumentor().instrument_app(app)
    RequestsInstrumentor().instrument()

def convert_otel_trace_id_to_xray(otel_trace_id_decimal):
    otel_trace_id_hex = "{:032x}".format(otel_trace_id_decimal)
    x_ray_trace_id = TRACE_ID_DELIMITER.join(
        [
            TRACE_ID_VERSION,
            otel_trace_id_hex[:TRACE_ID_FIRST_PART_LENGTH],
            otel_trace_id_hex[TRACE_ID_FIRST_PART_LENGTH:],
        ]
    )
    return '{{"traceId": "{}"}}'.format(x_ray_trace_id)

testingId = ""
if (os.environ.get("INSTANCE_ID")):
            testingId = "_" + os.environ["INSTANCE_ID"]

# Without instrumentation, the app would not have tracing
@app.route("/aws-sdk-call-no-instrumentation")
def aws_sdk_call_with_no_instrumentation():
    # Example Boto3 call to list S3 buckets
    s3 = boto3.client('s3')
    response = s3.list_buckets()
   

# With instrumentation, the app will have tracing
@app.route("/aws-sdk-call-with-instrumentation")
def aws_sdk_call_with_instrumentation():
    # Example Boto3 call to list S3 buckets
    with tracer.start_as_current_span("list_s3_buckets") as span:
        span.set_attribute("language", "python-manual-instrumentation")
        span.set_attribute("aws.service", "s3")
        span.set_attribute("aws.operation", "list_buckets")

        span.add_event("Starting to list S3 buckets")

        s3 = boto3.client('s3')
        response = s3.list_buckets()

        # Increment the count of AWS SDK calls with instrumentation
        aws_sdk_call_with_instrumentation_count.add(1)
        print("AWS SDK call with instrumentation count incremented")
        
        return {
            "message": "Listed S3 buckets successfully",
            "buckets": [bucket['Name'] for bucket in response['Buckets']],
            "traceId": convert_otel_trace_id_to_xray(
                int(span.get_span_context().trace_id)
            ) + testingId
        }
    


# Define a simple root endpoint
@app.route("/")
def root_endpoint():
    return "OK"

if __name__ == "__main__":
    setup_instrumentation()
    app.run(host="0.0.0.0", port=5000)