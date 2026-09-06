FROM python:3.13-slim

WORKDIR /backend

COPY requirements.txt /backend/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /backend/

ENV PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["python", "app.py"]
