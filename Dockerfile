FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY weather_data_fetcher.py .

CMD ["python", "weather_data_fetcher.py"]
