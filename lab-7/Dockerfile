FROM apache/airflow:2.9.3-python3.12

# Копирование зависимостей
COPY requirements.txt /tmp/requirements.txt

# Установка зависимостей с увеличенным таймаутом
RUN pip install --no-cache-dir --timeout 300 -r /tmp/requirements.txt || pip install --no-cache-dir --timeout 300 -r /tmp/requirements.txt
