# Sử dụng image Python chính thức
FROM python:3.12.3-alpine
# Cài đặt các phụ thuộc

WORKDIR /usr/src/app

RUN apk update && \
    apk add --no-cache build-base unixodbc-dev
# Sao chép file yêu cầu
COPY . .

# Cài đặt các gói phụ thuộc
RUN pip install --no-cache-dir -r requirements.txt

# Chạy ứng dụng
CMD ["python", "./app.py"]
