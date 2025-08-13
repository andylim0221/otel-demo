from opentelemetry.sdk.resources import Resource

# Import Traces
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Import Metrics
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

# Import Logs
from opentelemetry._logs import set_logger_provider, get_logger
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

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
resource = Resource.create({"service.name": "manual-instrumentation-demo"})

# Set up the tracer provider with the resource
trace_provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=otel_endpoint))
trace_provider.add_span_processor(processor)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)

# Set up the metric reader and exporter
metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=otel_endpoint), export_interval_millis=1000)
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)
metric = metrics.get_meter(__name__)

# Set up the logger provider
logger_provider = LoggerProvider(resource=resource)
set_logger_provider(logger_provider)
exporter = OTLPLogExporter(endpoint=otel_endpoint, insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
handler = LoggingHandler(level=logging.NOTSET,logger_provider=logger_provider)

logging.getLogger().setLevel(logging.NOTSET)
logging.getLogger().addHandler(handler)

s3_logger = logging.getLogger("boto3.resources.factory") 

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

# With instrumentation, the app will have tracing
@app.route("/aws-sdk-call-manual-instrumentation")
def aws_sdk_call_manual_instrumentation():

    start_time = datetime.now()
    current_span = trace.get_current_span()
    parent_context = current_span.get_span_context()

    s3_logger.info(f"Starting manual instrumentation for AWS SDK call Parent span: {parent_context.span_id}")

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
            s3_logger.error(f"Error listing S3 buckets: {str(e)}")
            aws_sdk_call_manual_instrumentation_count.add(1, {"operation": "list_buckets", "status": "error"})
            return {
                "error": str(e),
                "message": "Failed to list S3 buckets"
            }

if __name__ == "__main__":
    setup_instrumentation()
    app.run(host="0.0.0.0", port=port, debug=True)