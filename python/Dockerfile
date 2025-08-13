FROM python:3.13-alpine3.21

WORKDIR /app

COPY . .

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

RUN opentelemetry-bootstrap -a install

EXPOSE 5000

ENTRYPOINT ["opentelemetry-instrument", "python", "main.py"]