const express = require("express");
const sql = require("mssql");
const XLSX = require("xlsx");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3007;
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, PATCH, DELETE");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  next();
});

const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  port: parseInt(process.env.DB_PORT),
  options: {
    encrypt: true, // Sử dụng nếu SQL Server yêu cầu mã hóa
    trustServerCertificate: true, // Sử dụng nếu không có chứng chỉ SSL
  },
};

app.get("/download", async (req, res) => {
  try {
    let pool = await sql.connect(config);
    let result = await pool.request().query(`
            SELECT 
                d.month,
                SUM(fs.total_price) as total_price
            FROM fact_sales fs
            JOIN dim_date d ON fs.date_id = d.date_id
            WHERE d.year = 2024
            GROUP BY d.month
            ORDER BY d.month
        `);

    let monthlyRevenue = {};
    let totalRevenue = 0;

    result.recordset.forEach((record) => {
      monthlyRevenue[record.month] = record.total_price;
      totalRevenue += record.total_price;
    });

    // Đường dẫn tới tệp Excel của bạn
    const excelFilePath = path.join(__dirname, "report.xlsx");

    // Đọc tệp Excel
    const workbook = XLSX.readFile(excelFilePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];

    // Thay thế giá trị tại các ô xác định
    for (let i = 1; i <= 12; i++) {
      const placeholder = `{{thang${i}}}`;
      replaceCellValue(worksheet, placeholder, monthlyRevenue[i] || 0);
    }
    replaceCellValue(worksheet, "{{all}}", totalRevenue);

    // Tạo tệp Excel mới
    const newExcelFilePath = path.join(__dirname, "output_excel_file.xlsx");
    XLSX.writeFile(workbook, newExcelFilePath);

    // Gửi tệp Excel mới như một tệp tải xuống
    res.download(newExcelFilePath, "updated_excel_file.xlsx", (err) => {
      if (err) {
        console.error("Error downloading file:", err);
        res.status(500).send("Error downloading file");
      }
      // Xóa tệp Excel mới sau khi tải xuống
      fs.unlink(newExcelFilePath, (err) => {
        if (err) {
          console.error("Error deleting file:", err);
        }
      });
    });
  } catch (err) {
    console.error("SQL error", err);
    res.status(500).send("Error fetching data from database");
  }
});

function replaceCellValue(worksheet, findText, replaceText) {
  const range = XLSX.utils.decode_range(worksheet["!ref"]);
  for (let R = range.s.r; R <= range.e.r; ++R) {
    for (let C = range.s.c; C <= range.e.c; ++C) {
      const cellAddress = { c: C, r: R };
      const cellRef = XLSX.utils.encode_cell(cellAddress);
      const cell = worksheet[cellRef];
      if (cell && cell.t === "s" && cell.v.includes(findText)) {
        cell.v = cell.v.replace(findText, replaceText.toString());
      }
    }
  }
}

app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
});
