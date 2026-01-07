FROM python:3.12-slim

WORKDIR /app

COPY . .

EXPOSE 6000

CMD ["python", "server.py"]
