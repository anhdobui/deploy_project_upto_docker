import os
import mysql.connector
import pymssql
import time
import logging
from datetime import datetime

# Hàm kết nối MySQL
def connect_mysql():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST_MYSQL'),
        port=os.getenv('DB_PORT_MYSQL'),
        user=os.getenv('DB_USER_MYSQL'),
        password=os.getenv('DB_PASSWORD_MYSQL'),
        database=os.getenv('DB_NAME_MYSQL')
    )

# Hàm kết nối SQL Server
def connect_sqlserver():
    return pymssql.connect(
        server=os.getenv('DB_HOST_MSSQL'),
        user=os.getenv('DB_USER_MSSQL'),
        port=os.getenv('DB_PORT_MSSQL'),
        password=os.getenv('DB_PASSWORD_MSSQL'),
        database=os.getenv('DB_NAME_MSSQL')
    )

# Biến lưu trữ thời điểm cuối cùng quét dữ liệu cho từng bảng
last_sync_times = {
    "account": None,
    "cart": None,
    "cart_detail": None,
    "painting": None,
    "orders": None
}

# Hàm kiểm tra xem bảng có trường modifieddate không
def has_modified_date(mysql_cursor, table_name):
    query = f"SHOW COLUMNS FROM {table_name} LIKE 'modifieddate'"
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchone()
    return result is not None

def etl_process(mysql_cursor, sqlserver_cursor, table_name):
    try:
        last_sync_time = last_sync_times[table_name]
        if has_modified_date(mysql_cursor, table_name):
            if last_sync_time:
                query = f"SELECT * FROM {table_name} WHERE modifieddate > %s"
                mysql_cursor.execute(query, (last_sync_time,))
            else:
                query = f"SELECT * FROM {table_name}"
                mysql_cursor.execute(query)
            new_data = mysql_cursor.fetchall()
            etl_success_count = 0
            for row in new_data:
                columns = row.keys()
                columns_sql = ",".join(columns)
                placeholders = ",".join(["%s" for _ in range(len(columns))])
                values = tuple([row[col] for col in columns])
                # Tạo câu lệnh INSERT INTO và UPDATE riêng biệt
                insert_query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
                update_query = f"UPDATE {table_name} SET {', '.join([f'{col} = %s' for col in columns if col != 'id'])} WHERE id = %s"
                try:
                    sqlserver_cursor.execute(insert_query, values)
                    etl_success_count += 1
                except pymssql.IntegrityError:
                    # Nếu xảy ra lỗi trùng lặp
                    # Nếu xảy ra lỗi trùng lặp khóa chính, thực hiện cập nhật
                    update_values = tuple([row[col] for col in columns if col != 'id']) + (row['id'],)
                    sqlserver_cursor.execute(update_query, update_values)
                    etl_success_count += 1

            sqlserver_conn.commit()
            # Cập nhật thời gian chi tiết nhất có thể
            last_sync_times[table_name] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            logging.info(
                f"ETL process for {table_name} successful. ETL'd {etl_success_count} records."
            )

        # Đóng kết nối MySQL sau khi xử lý xong
    except Exception as e:
        logging.error(f"Error in ETL process for {table_name}: {str(e)}")

# Vòng lặp vô hạn để liên tục quét và xử lý dữ liệu
while True:
    try:
        mysql_conn = connect_mysql()
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        sqlserver_conn = connect_sqlserver()
        sqlserver_cursor = sqlserver_conn.cursor()

        for table_name in list(last_sync_times.keys()):
            etl_process(mysql_cursor, sqlserver_cursor, table_name)

        mysql_cursor.close()
        mysql_conn.close()
        sqlserver_cursor.close()
        sqlserver_conn.close()

        time.sleep(5)
    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
