FROM python:3.12-slim

WORKDIR /app

COPY requirements-lock.txt requirements.txt ./
RUN pip install --no-cache-dir -r requirements-lock.txt

COPY . .

ENTRYPOINT ["python", "main.py"]
