FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p data

EXPOSE 8000

CMD python -c "import os; port=os.environ.get('PORT','8000'); import uvicorn; uvicorn.run('main:app',host='0.0.0.0',port=int(port))"
