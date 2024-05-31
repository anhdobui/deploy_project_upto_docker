# Sử dụng hình ảnh SQL Server chính thức từ Microsoft
FROM mcr.microsoft.com/mssql/server:2022-latest


# Thư mục chứa các tập tin SQL
WORKDIR /usr/src/app

# Sao chép các tập tin SQL vào thư mục làm việc
COPY ./init.sql .

# Chạy tập tin SQL để khởi tạo schema và dữ liệu
CMD /opt/mssql/bin/sqlservr & sleep 20 & /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P Doba@2002 -i init.sql & tail -f /dev/null
