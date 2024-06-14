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
app.get("/api/lastDay", async (req, res) => {
  try {
    const { lastDay, measure } = req.query;

    // Validate lastDay parameter
    if (!lastDay || isNaN(parseInt(lastDay))) {
      return res.status(400).json({ error: "Invalid lastDay parameter" });
    }

    // Convert lastDay to an integer
    const days = parseInt(lastDay);

    // Connect to SQL Server
    await sql.connect(config);

    // Build dynamic CTE for LastDays
    let cteQuery = "";
    for (let i = 0; i < days; i++) {
      if (i > 0) cteQuery += " UNION ALL ";
      cteQuery += `SELECT CAST(DATEADD(day, -${i}, GETDATE()) AS DATE) AS [date${i + 1}]`;
    }

    // Common CTE and status part
    const commonCTE = `
        WITH LastDays AS (${cteQuery}),
        AllStatus AS (
            SELECT 'Completed' AS status
            UNION ALL
            SELECT 'Canceled'
            UNION ALL
            SELECT 'Delivery'
            UNION ALL
            SELECT 'Ordered'
        )
    `;

    // Build main query based on measure
    let mainQuery = "";
    if (measure !== "count") {
      mainQuery = `
            ${commonCTE}
            SELECT 
                dd.month_abbr,
                dd.day,
                s.status,
                COALESCE(cds.total_price, 0) AS total_revenue
            FROM 
                LastDays d
            CROSS JOIN 
                AllStatus s
            LEFT JOIN 
                dwh_htttql.dbo.dim_date dd ON d.date1 = dd.date -- Adjust date1 to the appropriate date column
            LEFT JOIN 
                cubes_htttql.dbo.cube_date_status cds ON dd.date_id = cds.date_id AND cds.status = s.status
            ORDER BY 
                dd.month_abbr desc,
                dd.day asc, 
                s.status
        `;
    } else {
      mainQuery = `
            ${commonCTE}
            SELECT 
                dd.month_abbr,
                dd.day,
                s.status,
                COUNT(cdo.orders_id) AS count_orders
            FROM 
                LastDays d
            CROSS JOIN 
                AllStatus s
            LEFT JOIN 
                dwh_htttql.dbo.dim_date dd ON d.date1 = dd.date -- Adjust date1 to the appropriate date column
            LEFT JOIN 
                cubes_htttql.dbo.cube_date_orders cdo ON dd.date_id = cdo.date_id AND cdo.status = s.status
            GROUP BY 
                dd.month_abbr,
                dd.day,
                s.status
            ORDER BY 
                dd.month_abbr desc,
                dd.day asc, 
                s.status
        `;
    }

    // Execute the query
    const result = await sql.query(mainQuery);

    // Close SQL connection
    await sql.close();

    // Send the result as a response
    res.json(result.recordset);
  } catch (error) {
    console.error("Error fetching data:", error.message);
    res.status(500).send("Error fetching data");
  }
});

app.get("/api/total-price", async (req, res) => {
  try {
    const { lastDay, status } = req.query;
    // Validate lastDay parameter
    if (!lastDay || isNaN(parseInt(lastDay))) {
      return res.status(400).json({ error: "Invalid lastDay parameter" });
    }
    // Convert lastDay to an integer
    const days = parseInt(lastDay);
    // Connect to SQL Server
    await sql.connect(config);

    // Build dynamic CTE for LastDays
    let cteQuery = "";
    for (let i = 0; i < days; i++) {
      if (i > 0) cteQuery += " UNION ALL ";
      cteQuery += `SELECT CAST(DATEADD(day, -${i}, GETDATE()) AS DATE) AS [date${i + 1}]`;
    }

    // Common CTE and status part
    const commonCTE = `
          WITH LastDays AS (${cteQuery}),
          AllStatus AS (
              SELECT 'Completed' AS status
              UNION ALL
              SELECT 'Canceled'
              UNION ALL
              SELECT 'Delivery'
              UNION ALL
              SELECT 'Ordered'
          )
      `;

    // Build query to calculate total revenue
    const mainQuery = `
          ${commonCTE}
          SELECT 
              SUM(COALESCE(cds.total_price, 0)) AS total_price
          FROM 
              LastDays d
          CROSS JOIN 
              AllStatus s
          LEFT JOIN 
              dwh_htttql.dbo.dim_date dd ON d.date1 = dd.date -- Adjust date1 to the appropriate date column
          LEFT JOIN 
              cubes_htttql.dbo.cube_date_status cds ON dd.date_id = cds.date_id AND cds.status = s.status
          ${status ? `where s.status = '${status}'` : ""}
      `;
    // Execute the query
    const result = await sql.query(mainQuery);

    // Close SQL connection
    await sql.close();

    // Send the result as a response
    res.json(result.recordset);
  } catch (err) {
    // Handle any errors
    console.error("Error executing SQL query:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});
app.get("/api/total-live-day", async (req, res) => {
  try {
    const { status } = req.query;
    // Connect to SQL Server
    await sql.connect(config);
    console.log(status);
    // SQL query to get total revenue for the current day
    const query = status
      ? `SELECT status,COALESCE(SUM(total_price), 0) AS total_revenue
FROM [cubes_htttql].[dbo].[cube_date_status]
WHERE date_id = CONVERT(VARCHAR(8), GETDATE(), 112) and status ='${status}'
group by status;`
      : `
          SELECT COALESCE(SUM(total_price), 0) AS total_revenue
          FROM [cubes_htttql].[dbo].[cube_date]
          WHERE date_id = CONVERT(VARCHAR(8), GETDATE(), 112);
      `;

    // Execute the query
    const result = await sql.query(query);

    // Close SQL connection
    await sql.close();

    // Send the result as a response
    res.json({ total_revenue: result.recordset[0].total_revenue });
  } catch (err) {
    // Handle any errors
    console.error("Error executing SQL query:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
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
