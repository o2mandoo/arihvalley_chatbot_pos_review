FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY review_chat_backend/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY review_chat_backend /app/review_chat_backend
RUN mkdir -p /app/review_analysis/data
COPY review_analysis/data /app/review_analysis/data
RUN mkdir -p /app/revenue-data
COPY revenue-data /app/revenue-data

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "review_chat_backend"]
