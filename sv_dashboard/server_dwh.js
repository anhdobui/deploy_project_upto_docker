const express = require("express");
const sql = require("mssql");
// require('dotenv').config(); // Import thư viện dotenv để đọc các biến môi trường từ tệp .env

const app = express();
const port = process.env.PORT || 3214;
// Cấu hình kết nối đến SQL Server
const config = {
  user: process.env.DB_USER_MSSQL, // Sử dụng biến môi trường
  password: process.env.DB_PASSWORD_MSSQL, // Sử dụng biến môi trường
  server: process.env.DB_HOST_MSSQL, // Sử dụng biến môi trường
  database: process.env.DB_NAME_MSSQL, // Sử dụng biến môi trường
  options: {
    encrypt: false, // Nếu bạn sử dụng kết nối bảo mật, hãy đặt encrypt thành true
  },
};

// Middleware để cho phép CORS
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, PATCH, DELETE");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  next();
});

// Route để lấy dữ liệu từ SQL Server
app.get("/api/:table_name", async (req, res) => {
  try {
    const table_name = req.params.table_name;
    const querys = req.query;

    let conditions = Object.entries(querys)
      .filter(([key, value]) => key !== "orderby_desc" && key !== "orderby" && key !== "limit")
      .reduce((obj, [key, value]) => {
        obj[key] = value;
        return obj;
      }, {});

    const orderby = querys["orderby"] || querys["orderby_desc"];
    const byDesc = !!querys["orderby_desc"] ? " Desc" : "";
    let limit = null;
    if (querys["limit"]) {
      limit = parseInt(querys["limit"]);
      if (isNaN(limit)) {
        console.error("Limit is not a number");
        // Bạn có thể xử lý lỗi ở đây hoặc gán một giá trị mặc định cho limit.
        // Ví dụ: limit = 10;
      }
    }

    const pool = await sql.connect(config);
    let where = "";
    let params = {};

    if (conditions) {
      where = " WHERE 1=1 ";
      const conditionKeys = Object.keys(conditions);
      conditionKeys.forEach((key, index) => {
        where += `AND ${key} = @${key}`;
        params[key] = { type: sql.VarChar, value: conditions[key] };
        if (index < conditionKeys.length - 1) {
          where += " ";
        }
      });
    }

    const sql_query = `SELECT ${table_name}.* FROM ${table_name}
                        ${where}
                        ${orderby ? "ORDER BY " + orderby + byDesc : ""}
                        ${limit ? "OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY" : ""}`;

    const request = pool.request();
    Object.keys(params).forEach((key) => {
      request.input(key, params[key].type, params[key].value);
    });

    const result = await request.query(sql_query);
    res.json(result.recordset);
  } catch (err) {
    console.error("Error occurred:", err);
    res.status(500).json({ error: "An error occurred while fetching data" });
  }
});

// Khởi động máy chủ
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
