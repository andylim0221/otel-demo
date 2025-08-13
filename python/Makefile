.PHONY: run-all clean-all

run-all:
	@echo "Running instrumentation demo..."
	docker-compose -f docker-compose.yml up --build -d
	docker ps
	@echo "Autoinstrumentation demo is running. Access it at http://localhost:5000/aws-sdk-call-auto-instrumentation"
	@echo "Manual instrumentation demo is running. Access it at http://localhost:5003/aws-sdk-call-manual-instrumentation"

clean-all:
	@echo "Cleaning up instrumentation demo..."
	docker-compose -f docker-compose.yml down --remove-orphans