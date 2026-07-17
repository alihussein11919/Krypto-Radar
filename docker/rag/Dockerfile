FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-rag.txt .
RUN pip install --no-cache-dir -r requirements-rag.txt

COPY src/ /app/src/

EXPOSE 8000

CMD ["uvicorn", "src.rag.api:app", "--host", "0.0.0.0", "--port", "8000"]
