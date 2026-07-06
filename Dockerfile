FROM python:3.11-slim

WORKDIR /app
COPY node.py .
COPY libs/ ./libs/

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "node.py"]
