# Sử dụng image node chính thức
FROM node

# Thiết lập thư mục làm việc
WORKDIR /app

RUN npm install express

COPY dist ./dist
COPY server_admin.js ./server.js

# Thiết lập biến môi trường để ứng dụng Node.js sử dụng cổng
ENV PORT=3000

# Expose cổng ứng dụng
EXPOSE 3000

# Chạy ứng dụng
CMD ["node", "server.js"]
