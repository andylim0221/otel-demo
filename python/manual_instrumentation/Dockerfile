FROM python:3.13-alpine3.21

WORKDIR /app

COPY . .

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5003

ENTRYPOINT ["python", "main.py"]