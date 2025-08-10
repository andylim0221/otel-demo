.PHONY: run-all clean-all

run-all:
	@echo "Running instrumentation demo..."
	docker-compose -f docker-compose-manual.yml up --build -d
	docker ps
	@echo "instrumentation demo is running. Access it at http://localhost:5000/aws-sdk-call-with-instrumentation"

clean-all:
	@echo "Cleaning up instrumentation demo..."
	docker-compose -f docker-compose-manual.yml down --remove-orphans