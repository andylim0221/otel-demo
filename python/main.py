from opentelemetry import trace, metrics, _logs
from opentelemetry.sdk.resources import Resource

# Import Traces
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Import Metrics
from opentelemetry.sdk.metrics import MeterProvider

# Import Logs
from opentelemetry._logs import set_logger_provider, get_logger
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

# Import Span Exporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Propogators
from opentelemetry.propagators.aws.aws_xray_propagator import (
    TRACE_ID_DELIMITER,
    TRACE_ID_FIRST_PART_LENGTH,
    TRACE_ID_VERSION,
)

# Instrumentation libraries
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

# Flask application
from flask import Flask
import boto3
import os
import logging
from datetime import datetime

# Environment variable for OpenTelemetry exporter endpoint
otel_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
port = os.environ.get("PORT", "5000")

# Set up OpenTelemetry
resource = Resource.create({"service.name": "instrumentation-demo"})

# Set up the tracer provider with the resource
trace_provider = TracerProvider(resource=resource)
metric_provider = MeterProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=otel_endpoint))
trace_provider.add_span_processor(processor)

# Set up the logger provider
logger_provider = LoggerProvider(resource=resource)
exporter = OTLPLogExporter(endpoint=otel_endpoint, insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
set_logger_provider(logger_provider)
s3_bucket_logger = get_logger("s3_bucket_logger")

metrics.set_meter_provider(metric_provider)
trace.set_tracer_provider(trace_provider)

tracer = trace.get_tracer(__name__)
metric = metrics.get_meter(__name__)

handler = LoggingHandler(level=logging.DEBUG,logger_provider=logger_provider)

# Initialize Flask application
app = Flask(__name__)
app.logger.addHandler(handler)

aws_sdk_call_manual_instrumentation_count = metric.create_counter(
    "aws_sdk_call_manual_instrumentation_count",
    unit="1",
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
@app.route("/aws-sdk-call-auto-instrumentation")
def aws_sdk_call_with_auto_instrumentation():
    # Example Boto3 call to list S3 buckets
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    return {
        "message": "Listed S3 buckets successfully",
        "buckets": [bucket['Name'] for bucket in response['Buckets']]
    }
   

# With instrumentation, the app will have tracing
@app.route("/aws-sdk-call-manual-instrumentation")
def aws_sdk_call_manual_instrumentation():

    start_time = datetime.now()
    current_span = trace.get_current_span()
    parent_context = current_span.get_span_context()

    logging.info(f"Starting manual instrumentation for AWS SDK call Parent span: {parent_context.span_id}")

    # Main operation span
    with tracer.start_as_current_span("aws_s3_list_operation") as main_span:
        try:
            main_span.set_attribute("operation.type", "aws_sdk_call")
            main_span.set_attribute("language", "python-manual-instrumentation")
            main_span.set_attribute("aws.service", "s3")
            main_span.set_attribute("aws.operation", "list_buckets")
            main_span.set_attribute("component", "boto3")

            main_span.add_event("Operation initialized", {"timestamp": start_time.isoformat()})

            with tracer.start_as_current_span("s3_client_setup") as client_span:
                client_span.set_attribute("aws.service", "s3")
                client_span.set_attribute("operation.phase", "client_initialization")
                
                client_span.add_event("Starting S3 client initialization")
                
                s3 = boto3.client('s3')
                
                client_span.add_event("S3 client initialized successfully")
            
            with tracer.start_as_current_span("s3_list_buckets_api_call") as api_span:
                api_span.set_attribute("aws.service", "s3")
                api_span.set_attribute("aws.operation", "list_buckets")
                api_span.set_attribute("operation.phase", "api_execution")
                api_span.set_attribute("http.method", "GET")
                
                api_span.add_event("Starting API call to list S3 buckets")
                
                api_call_start = datetime.now()
                response = s3.list_buckets()
                api_call_end = datetime.now()
                api_duration = (api_call_end - api_call_start).total_seconds() * 1000
                
                bucket_count = len(response['Buckets'])
                api_span.set_attribute("aws.s3.bucket_count", bucket_count)
                api_span.set_attribute("operation.duration_ms", api_duration)
                
                api_span.add_event("API call completed successfully", {
                    "bucket_count": bucket_count,
                    "duration_ms": api_duration
                })

            with tracer.start_as_current_span("response_processing") as processing_span:
                processing_span.set_attribute("operation.phase", "data_processing")
                processing_span.set_attribute("data.type", "s3_bucket_list")
                
                processing_span.add_event("Starting response data processing")
                
                bucket_names = [bucket['Name'] for bucket in response['Buckets']]
                
                processing_span.set_attribute("processed.bucket_count", len(bucket_names))
                processing_span.add_event("Response processing completed", {
                    "processed_buckets": len(bucket_names)
                })

            # Calculate total operation duration
            end_time = datetime.now()
            total_duration = (end_time - start_time).total_seconds() * 1000

            main_span.set_attribute("operation.total_duration_ms", total_duration)
            main_span.set_attribute("operation.success", True)
            main_span.add_event("Operation completed successfully", {
                "total_duration_ms": total_duration,
                "buckets_found": len(bucket_names),
                "end_timestamp": end_time.isoformat()
            })

            # Increment the count of AWS SDK calls with instrumentation
            aws_sdk_call_manual_instrumentation_count.add(1, {"operation": "list_buckets"})
            
            return {
                "message": "Listed S3 buckets successfully",
                "buckets": bucket_names,
            }

        except Exception as e:
            main_span.record_exception(e)
            main_span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            aws_sdk_call_manual_instrumentation_count.add(1, {"operation": "list_buckets", "status": "error"})
            s3_bucket_logger.error(f"Error during AWS SDK call: {str(e)}")
            return {
                "error": str(e),
                "message": "Failed to list S3 buckets"
            }
    


# Define a simple root endpoint
@app.route("/")
def root_endpoint():
    return "OK"

if __name__ == "__main__":
    setup_instrumentation()
    app.run(host="0.0.0.0", port=port, debug=True)