FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Mặc định chạy live 20 phút; override bằng docker run ... hoặc
# đổi entrypoint sang replay.py khi cần test 10k/100k
ENTRYPOINT ["python3", "-u", "main.py"]
CMD ["--duration", "1200"]
