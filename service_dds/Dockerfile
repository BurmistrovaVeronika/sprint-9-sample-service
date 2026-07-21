FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY certs/YandexInternalRootCA.crt /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt

CMD ["python", "app.py"]
