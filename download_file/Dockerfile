# Sử dụng image node chính thức
FROM node

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy tất cả các tệp từ thư mục local của bạn vào thư mục làm việc trong container
COPY . .

# Cài đặt các gói npm sau khi đã sao chép tất cả các tệp vào container
RUN npm install express mssql xlsx dotenv

# Chạy ứng dụng
CMD ["node", "download_file.js"]
