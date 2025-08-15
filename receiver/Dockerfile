FROM python:3.12-slim
WORKDIR /app

RUN pip install uv

# Copy pyproject.toml from build context
RUN uv pip install --system fastapi==0.104.1 uvicorn==0.24.0 redis==5.0.1

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]