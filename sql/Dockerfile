# Sử dụng hình ảnh SQL Server chính thức từ Microsoft
FROM mcr.microsoft.com/mssql/server:2022-latest

# Đặt biến môi trường cho mật khẩu SA và chấp nhận thỏa thuận EULA
ENV SA_PASSWORD=Doba@2002
ENV ACCEPT_EULA=Y

# Tạo thư mục làm việc và sao chép tệp SQL vào thư mục này
WORKDIR /usr/src/app
COPY ./init.sql .

# Chạy SQL Server và chờ đợi để đảm bảo SQL Server đã khởi động hoàn toàn trước khi chạy tệp SQL
CMD /bin/bash -c "/opt/mssql/bin/sqlservr & sleep 30 && /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SA_PASSWORD} -i /usr/src/app/init.sql && tail -f /dev/null"
