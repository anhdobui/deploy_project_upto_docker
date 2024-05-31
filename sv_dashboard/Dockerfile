# Sử dụng image node chính thức
FROM node

# Thiết lập thư mục làm việc
WORKDIR /app

COPY . .

RUN npm install express mssql 

# Chạy ứng dụng
CMD ["node", "server_dwh.js"]
