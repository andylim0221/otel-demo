.PHONY: run-manual run-auto clean-manual clean-auto clean-all

run-manual:
	@echo "Running manual instrumentation demo..."
	docker-compose -f docker-compose-manual.yml up --build -d
	@echo "Manual instrumentation demo is running. Access it at http://localhost:5000/aws-sdk-call-with-instrumentation"

clean-manual:
	@echo "Cleaning up manual instrumentation demo..."
	docker-compose -f docker-compose-manual.yml down --remove-orphans