# Sử dụng image OpenJDK làm base image
FROM openjdk:17-jdk-alpine

# Tạo thư mục trong container để chứa ứng dụng của bạn
WORKDIR /app

# Copy file JAR của ứng dụng vào thư mục /app
COPY server.jar server.jar

# Chạy ứng dụng Spring Boot của bạn
CMD ["java", "-jar", "server.jar"]