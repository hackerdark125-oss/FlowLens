FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY flowlens ./flowlens
RUN pip install --no-cache-dir .
ENV FLOWLENS_DB_PATH=/data/flowlens.db
RUN mkdir -p /data
EXPOSE 8000
CMD ["uvicorn", "flowlens.main:app", "--host", "0.0.0.0", "--port", "8000"]
