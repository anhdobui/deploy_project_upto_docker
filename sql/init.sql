CREATE DATABASE [staging];
go
CREATE DATABASE [dwh_htttql];
go
CREATE DATABASE [cubes_htttql];
go
use staging;
go
create FUNCTION [dbo].[fn_map_payment_status] (@source_payment_status INT)
RETURNS NVARCHAR(20)
AS
BEGIN
    DECLARE @mapped_payment_status NVARCHAR(20);

    IF @source_payment_status = 0
        SET @mapped_payment_status = 'Unpaid';
    ELSE IF @source_payment_status = 1
        SET @mapped_payment_status = 'Paid';
    ELSE IF @source_payment_status = 2
        SET @mapped_payment_status = 'Refunded';
    ELSE
        SET @mapped_payment_status = 'Unknown';

    RETURN @mapped_payment_status;
END;
GO
create FUNCTION [dbo].[fn_map_status] (@source_status INT)
RETURNS NVARCHAR(20)
AS
BEGIN
    DECLARE @mapped_status NVARCHAR(20);

    IF @source_status = 0
        SET @mapped_status = 'Canceled';
    ELSE IF @source_status = 1
        SET @mapped_status = 'Ordered';
    ELSE IF @source_status = 2
        SET @mapped_status = 'Delivery';
    ELSE IF @source_status = 3
        SET @mapped_status = 'Completed';
    ELSE
        SET @mapped_status = 'Unknown';

    RETURN @mapped_status;
END;
GO
CREATE TABLE [dbo].[account](
	[id] [bigint] NOT NULL,
	[createdby] [nvarchar](255) NULL,
	[createddate] [datetime2](6) NULL,
	[modifiedby] [nvarchar](255) NULL,
	[modifieddate] [datetime2](6) NULL,
	[city] [nvarchar](255) NULL,
	[district] [nvarchar](255) NULL,
	[email] [nvarchar](255) NULL,
	[fullname] [nvarchar](255) NULL,
	[grade] [float] NULL,
	[password] [nvarchar](255) NULL,
	[phone] [nvarchar](255) NULL,
	[point_address] [nvarchar](255) NULL,
	[username] [nvarchar](255) NULL,
	[ward] [nvarchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE TABLE [dbo].[cart](
	[id] [bigint] NOT NULL,
	[createdby] [nvarchar](255) NULL,
	[createddate] [datetime2](6) NULL,
	[modifiedby] [nvarchar](255) NULL,
	[modifieddate] [datetime2](6) NULL,
	[status] [int] NULL,
	[acc_id] [bigint] NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE TABLE [dbo].[cart_detail](
	[id] [bigint] NOT NULL,
	[createdby] [nvarchar](255) NULL,
	[createddate] [datetime2](6) NULL,
	[modifiedby] [nvarchar](255) NULL,
	[modifieddate] [datetime2](6) NULL,
	[qty] [int] NULL,
	[cart_id] [bigint] NULL,
	[painting_id] [bigint] NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE TABLE [dbo].[orders](
	[id] [bigint] NOT NULL,
	[createdby] [nvarchar](255) NULL,
	[createddate] [datetime2](6) NULL,
	[modifiedby] [nvarchar](255) NULL,
	[modifieddate] [datetime2](6) NULL,
	[cancellation_date] [datetime2](6) NULL,
	[code] [nvarchar](255) NULL,
	[delivery_date] [datetime2](6) NULL,
	[finished_date] [datetime2](6) NULL,
	[order_date] [datetime2](6) NULL,
	[status] [int] NULL,
	[cart_id] [bigint] NULL,
	[delivery_address] [nvarchar](255) NULL,
	[payment_status] [int] NULL,
	[shipping_cost] [float] NULL,
 CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE TABLE [dbo].[painting](
	[id] [bigint] NOT NULL,
	[createdby] [nvarchar](255) NULL,
	[createddate] [datetime2](6) NULL,
	[modifiedby] [nvarchar](255) NULL,
	[modifieddate] [datetime2](6) NULL,
	[code] [nvarchar](255) NULL,
	[inventory] [int] NULL,
	[length] [float] NULL,
	[name] [nvarchar](255) NULL,
	[price] [float] NULL,
	[thickness] [float] NULL,
	[thumbnail_url] [nvarchar](255) NULL,
	[width] [float] NULL,
	[artist] [nvarchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
-- procedure
CREATE PROCEDURE [dbo].[ETL_Update_Dim_Customer]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @currentDateTime DATETIME;
    SET @currentDateTime = GETDATE();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Log the start of the procedure
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, status)
        VALUES ('ETL_Update_Dim_Customer', @currentDateTime, 'Started');

        -- Step 1: Update existing records that have changed
        UPDATE target
        SET ending_date = @currentDateTime
        FROM dwh_htttql.dbo.dim_customer AS target
        JOIN staging.dbo.account AS source
        ON target.customer_id = source.id
        WHERE target.ending_date IS NULL
            AND (
                target.fullname <> source.fullname OR
                target.email <> source.email OR
                target.phone <> source.phone OR
                target.username <> source.username OR
                target.point_address <> source.point_address OR
                target.ward <> source.ward OR
                target.district <> source.district OR
                target.city <> source.city OR
                target.grade <> source.grade
            );

        -- Log the update step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Customer', @currentDateTime, GETDATE(), 'Step 1: Updated existing records');

        -- Step 2: Insert new records
        INSERT INTO dwh_htttql.dbo.dim_customer (
            customer_id,
            fullname,
            email,
            phone,
            username,
            point_address,
            ward,
            district,
            city,
            grade,
            status_flag,
            starting_date,
            ending_date
        )
        SELECT 
            source.id AS customer_id,
            source.fullname,
            source.email,
            source.phone,
            source.username,
            source.point_address,
            source.ward,
            source.district,
            source.city,
            source.grade,
            1, -- Assuming 1 as active status flag
            @currentDateTime AS starting_date,
            NULL AS ending_date
        FROM staging.dbo.account AS source
        WHERE NOT EXISTS (
            SELECT 1
            FROM dwh_htttql.dbo.dim_customer AS target
            WHERE target.customer_id = source.id
              AND target.ending_date IS NULL
        );

        -- Log the insert step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Customer', @currentDateTime, GETDATE(), 'Step 2: Inserted new records');

        -- Step 3: Update obsolete records
        UPDATE dwh_htttql.dbo.dim_customer
        SET status_flag = 0
        WHERE ending_date IS NOT NULL
            AND status_flag = 1;

        -- Log the update obsolete records step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Customer', @currentDateTime, GETDATE(), 'Step 3: Updated obsolete records');

        -- Log the end of the procedure
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Customer', @currentDateTime, GETDATE(), 'Ended');

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        -- Log the error
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status, error_message)
        VALUES ('ETL_Update_Dim_Customer', @currentDateTime, GETDATE(), 'Failed', ERROR_MESSAGE());
    END CATCH;
END;
GO
CREATE PROCEDURE [dbo].[ETL_Update_Dim_Orders]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @currentDateTime DATETIME;
    SET @currentDateTime = GETDATE();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Log the start of the procedure
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, status)
        VALUES ('ETL_Update_Dim_Orders', @currentDateTime, 'Started');

        -- Step 1: Update existing records that have changed
        UPDATE target
        SET ending_date = @currentDateTime
        FROM dwh_htttql.dbo.dim_orders AS target
        JOIN staging.dbo.orders AS source
        ON target.orders_id = source.id
        WHERE target.ending_date IS NULL
            AND (
                target.code <> source.code OR
                target.status <> dbo.fn_map_status(source.status) OR
                target.delivery_address <> source.delivery_address OR
                target.payment_status <> dbo.fn_map_payment_status(source.payment_status) OR
                target.shipping_cost <> source.shipping_cost
            );

        -- Log the update step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Orders', @currentDateTime, GETDATE(), 'Step 1: Updated existing records');

        -- Step 2: Insert new records
        INSERT INTO dwh_htttql.dbo.dim_orders (
            orders_id,
            code,
            status,
            delivery_address,
            payment_status,
            shipping_cost,
            status_flag,
            starting_date,
            ending_date
        )
        SELECT 
            source.id AS orders_id,
            source.code,
            dbo.fn_map_status(source.status),
            source.delivery_address,
            dbo.fn_map_payment_status(source.payment_status),
            source.shipping_cost,
            1, -- Assuming 1 as active status flag
            @currentDateTime AS starting_date,
            NULL AS ending_date
        FROM staging.dbo.orders AS source
        WHERE NOT EXISTS (
            SELECT 1
            FROM dwh_htttql.dbo.dim_orders AS target
            WHERE target.orders_id = source.id
              AND target.ending_date IS NULL
        );

        -- Log the insert step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Orders', @currentDateTime, GETDATE(), 'Step 2: Inserted new records');

        -- Step 3: Update obsolete records
        UPDATE dwh_htttql.dbo.dim_orders
        SET status_flag = 0
        WHERE ending_date IS NOT NULL
            AND status_flag = 1;

        -- Log the update obsolete records step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Orders', @currentDateTime, GETDATE(), 'Step 3: Updated obsolete records');

        -- Log the end of the procedure
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Orders', @currentDateTime, GETDATE(), 'Ended');

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        -- Logging error
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status, error_message)
        VALUES ('ETL_Update_Dim_Orders', @currentDateTime, GETDATE(), 'Failed', ERROR_MESSAGE());
    END CATCH;
END;
GO
CREATE PROCEDURE [dbo].[ETL_Update_Dim_Painting]
AS
BEGIN
    -- Log the start of the procedure
    DECLARE @StartTime DATETIME = GETDATE();
    INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, status)
    VALUES ('ETL_Update_Dim_Painting', @StartTime, 'Started');

    BEGIN TRY
        -- Step 1: Update existing records
        UPDATE target
        SET end_date = GETDATE()
        FROM dwh_htttql.dbo.dim_painting AS target
        JOIN staging.dbo.painting AS source
        ON target.painting_id = source.id
        WHERE target.end_date IS NULL
            AND (
                target.code <> source.code OR
                target.length <> source.length OR
                target.thickness <> source.thickness OR
                target.width <> source.width OR
                target.name <> source.name OR
                target.artist <> source.artist OR
                target.price <> source.price OR
                target.thumbnail_url <> source.thumbnail_url
            );

        -- Log the update existing records step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Painting', @StartTime, GETDATE(), 'Step 1: Updated existing records');

        -- Step 2: Insert new records
        INSERT INTO dwh_htttql.dbo.dim_painting (
            painting_id,
            code,
            length,
            thickness,
            width,
            name,
            artist,
            price,
            thumbnail_url,
            status_flag,
            starting_date,
            end_date
        )
        SELECT 
            source.id AS painting_id,
            source.code,
            source.length,
            source.thickness,
            source.width,
            source.name,
            source.artist,
            source.price,
            source.thumbnail_url,
            1, -- Assuming 1 as active status flag
            GETDATE() AS starting_date,
            NULL AS end_date
        FROM staging.dbo.painting AS source
        LEFT JOIN dwh_htttql.dbo.dim_painting AS target
        ON target.painting_id = source.id
        WHERE NOT EXISTS (
            SELECT 1
            FROM dwh_htttql.dbo.dim_painting AS target
            WHERE target.painting_id = source.id
              AND target.end_date IS NULL
        );

        -- Log the insert new records step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Painting', @StartTime, GETDATE(), 'Step 2: Inserted new records');

        -- Step 3: Update obsolete records
        UPDATE dwh_htttql.dbo.dim_painting
        SET status_flag = 0
        WHERE end_date IS NOT NULL
            AND status_flag = 1;

        -- Log the update obsolete records step completion
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Painting', @StartTime, GETDATE(), 'Step 3: Updated obsolete records');

        -- Log the end of the procedure
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Dim_Painting', @StartTime, GETDATE(), 'Completed');
    END TRY
    BEGIN CATCH
        -- Log the error if any
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status, error_message)
        VALUES ('ETL_Update_Dim_Painting', @StartTime, GETDATE(), 'Error', ERROR_MESSAGE());
    END CATCH;
END;
GO
CREATE PROCEDURE [dbo].[ETL_Update_Fact_Sales_RealTime]
    @DateToProcess DATE = NULL
AS
BEGIN
    -- Log the start of the procedure
    INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, status)
    VALUES ('ETL_Update_Fact_Sales_RealTime', GETDATE(), 'Started');

    BEGIN TRY
        -- Update existing records
        UPDATE fs
        SET fs.customer_id = source.customer_id,
            fs.painting_id = source.painting_id,
            fs.total_price = source.total_price
        FROM dwh_htttql.dbo.fact_sales fs
        INNER JOIN (
            SELECT 
                c.acc_id AS customer_id,
                cd.painting_id,
                CONVERT(INT, CONVERT(VARCHAR(8), o.order_date, 112)) AS date_id,
                o.id AS orders_id,
                cd.qty * p.price AS total_price
            FROM staging.dbo.cart_detail cd
            INNER JOIN staging.dbo.cart c ON cd.cart_id = c.id
            INNER JOIN staging.dbo.orders o ON c.id = o.cart_id
            INNER JOIN staging.dbo.painting p ON cd.painting_id = p.id
            INNER JOIN staging.dbo.account a ON c.acc_id = a.id  
            WHERE (@DateToProcess IS NULL OR o.order_date = @DateToProcess)
        ) AS source ON fs.date_id = source.date_id
            AND fs.orders_id = source.orders_id
            AND fs.customer_id = source.customer_id
            AND fs.painting_id = source.painting_id; 

        -- Insert new records
        INSERT INTO dwh_htttql.dbo.fact_sales (customer_id, painting_id, date_id, orders_id, total_price)
        SELECT source.customer_id, source.painting_id, source.date_id, source.orders_id, source.total_price
        FROM (
            SELECT 
                c.acc_id AS customer_id,
                cd.painting_id,
                CONVERT(INT, CONVERT(VARCHAR(8), o.order_date, 112)) AS date_id,
                o.id AS orders_id,
                cd.qty * p.price AS total_price
            FROM staging.dbo.cart_detail cd
            INNER JOIN staging.dbo.cart c ON cd.cart_id = c.id
            INNER JOIN staging.dbo.orders o ON c.id = o.cart_id
            INNER JOIN staging.dbo.painting p ON cd.painting_id = p.id
            INNER JOIN staging.dbo.account a ON c.acc_id = a.id  
            WHERE (@DateToProcess IS NULL OR o.order_date = @DateToProcess)
        ) AS source
        LEFT JOIN dwh_htttql.dbo.fact_sales fs ON fs.date_id = source.date_id
            AND fs.orders_id = source.orders_id
            AND fs.customer_id = source.customer_id
            AND fs.painting_id = source.painting_id  
        WHERE fs.date_id IS NULL;

        -- Log the completion of the process
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status)
        VALUES ('ETL_Update_Fact_Sales_RealTime', GETDATE(), GETDATE(), 'Completed');
    END TRY
    BEGIN CATCH
        -- Log the error if any
        INSERT INTO dwh_htttql.dbo.etl_log (process_name, start_time, end_time, status, error_message)
        VALUES ('ETL_Update_Fact_Sales_RealTime', GETDATE(), GETDATE(), 'Error', ERROR_MESSAGE());
    END CATCH;
END;
GO
CREATE TRIGGER [dbo].[trg_Update_Dim_Customer]
ON [dbo].[account]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Gọi stored procedure ETL_Update_Dim_Customer
    EXEC dbo.ETL_Update_Dim_Customer;
END;
GO
CREATE TRIGGER [dbo].[trg_Update_Dim_Orders]
ON [dbo].[orders]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Gọi stored procedure ETL_Update_Dim_Orders
    EXEC dbo.ETL_Update_Dim_Orders;
	EXEC dbo.ETL_Update_Fact_Sales_RealTime;
END;
GO
CREATE TRIGGER [dbo].[trg_Update_Dim_Painting]
ON [dbo].[painting]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Gọi stored procedure ETL_Update_Dim_Painting
    EXEC dbo.ETL_Update_Dim_Painting;
END;
GO
use dwh_htttql;
CREATE TABLE [dbo].[dim_customer](
	[customer_scd_id] [int] IDENTITY(1,1) NOT NULL,
	[customer_id] [int] NULL,
	[fullname] [nvarchar](255) NULL,
	[email] [nvarchar](255) NULL,
	[phone] [nvarchar](50) NULL,
	[username] [nvarchar](50) NULL,
	[point_address] [nvarchar](255) NULL,
	[ward] [nvarchar](255) NULL,
	[district] [nvarchar](255) NULL,
	[city] [nvarchar](255) NULL,
	[grade] [nvarchar](50) NULL,
	[status_flag] [int] NULL,
	[starting_date] [datetime] NULL,
	[ending_date] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[customer_scd_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE TABLE [dbo].[dim_date](
	[date_id] [int] NOT NULL,
	[date] [date] NULL,
	[day] [int] NULL,
	[week] [int] NULL,
	[month] [int] NULL,
	[quarter] [int] NULL,
	[year] [int] NULL
) ON [PRIMARY]
GO
DECLARE @startDate DATE = '2020-01-01';
DECLARE @endDate DATE = '2025-12-31';

WHILE @startDate <= @endDate
BEGIN
    INSERT INTO dbo.dim_date (date_id, [date], [day], [week], [month], [quarter], [year])
    VALUES (
        CAST(FORMAT(@startDate, 'yyyyMMdd') AS INT),
        @startDate,
        DATEPART(DAY, @startDate),
        DATEPART(WEEK, @startDate),
        DATEPART(MONTH, @startDate),
        DATEPART(QUARTER, @startDate),
        DATEPART(YEAR, @startDate)
    );

    -- Tăng giá trị của @startDate lên một ngày
    SET @startDate = DATEADD(DAY, 1, @startDate);
END;
GO
CREATE TABLE [dbo].[dim_orders](
	[orders_scd_id] [int] IDENTITY(1,1) NOT NULL,
	[orders_id] [int] NULL,
	[code] [nvarchar](50) NULL,
	[status] [nvarchar](20) NULL,
	[delivery_address] [nvarchar](255) NULL,
	[payment_status] [nvarchar](20) NULL,
	[shipping_cost] [decimal](18, 2) NULL,
	[status_flag] [int] NULL,
	[starting_date] [datetime] NULL,
	[ending_date] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[orders_scd_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dwh_htttql].[dbo].[dim_date]
ADD month_abbr VARCHAR(3);
GO
-- Cập nhật dữ liệu cho cột month_abbr
UPDATE [dwh_htttql].[dbo].[dim_date]
SET month_abbr = 
    CASE DATENAME(month, [date])
        WHEN 'January' THEN 'Jan'
        WHEN 'February' THEN 'Feb'
        WHEN 'March' THEN 'Mar'
        WHEN 'April' THEN 'Apr'
        WHEN 'May' THEN 'May'
        WHEN 'June' THEN 'Jun'
        WHEN 'July' THEN 'Jul'
        WHEN 'August' THEN 'Aug'
        WHEN 'September' THEN 'Sep'
        WHEN 'October' THEN 'Oct'
        WHEN 'November' THEN 'Nov'
        WHEN 'December' THEN 'Dec'
        ELSE ''
    END;
GO
CREATE TABLE [dbo].[dim_painting](
	[painting_scd_id] [int] IDENTITY(1,1) NOT NULL,
	[painting_id] [int] NULL,
	[code] [nvarchar](50) NULL,
	[length] [decimal](18, 2) NULL,
	[thickness] [decimal](18, 2) NULL,
	[width] [decimal](18, 2) NULL,
	[name] [nvarchar](255) NULL,
	[artist] [nvarchar](255) NULL,
	[price] [decimal](18, 2) NULL,
	[thumbnail_url] [nvarchar](255) NULL,
	[status_flag] [int] NULL,
	[starting_date] [datetime] NULL,
	[end_date] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[painting_scd_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE TABLE [dbo].[etl_log](
	[log_id] [int] IDENTITY(1,1) NOT NULL,
	[process_name] [nvarchar](100) NULL,
	[start_time] [datetime] NULL,
	[end_time] [datetime] NULL,
	[status] [nvarchar](50) NULL,
	[error_message] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[log_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
CREATE TABLE [dbo].[fact_sales](
	[fact_sales_id] [int] IDENTITY(1,1) NOT NULL,
	[customer_id] [int] NULL,
	[painting_id] [int] NULL,
	[date_id] [int] NULL,
	[orders_id] [int] NULL,
	[total_price] [decimal](18, 2) NULL,
PRIMARY KEY CLUSTERED 
(
	[fact_sales_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
     CREATE PROCEDURE [dbo].[ProcessNewValuesFact_sales]
     AS
     BEGIN 
     
            TRUNCATE TABLE [cubes_htttql].[dbo].cube_;
            INSERT INTO [cubes_htttql].[dbo].cube_
            SELECT  sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            ;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_orders
            SELECT do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_status;
            INSERT INTO [cubes_htttql].[dbo].cube_status
            SELECT do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_painting
            SELECT dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_painting_orders
            SELECT dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_painting_status
            SELECT dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_customer
            SELECT dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_customer_orders
            SELECT dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_customer_status
            SELECT dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_customer_painting
            SELECT dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_customer_painting_orders
            SELECT dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_customer_painting_status
            SELECT dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date;
            INSERT INTO [cubes_htttql].[dbo].cube_date
            SELECT dd.date_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_date_orders
            SELECT dd.date_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_status;
            INSERT INTO [cubes_htttql].[dbo].cube_date_status
            SELECT dd.date_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_date_painting
            SELECT dd.date_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_date_painting_orders
            SELECT dd.date_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_date_painting_status
            SELECT dd.date_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_date_customer
            SELECT dd.date_id,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_date_customer_orders
            SELECT dd.date_id,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_date_customer_status
            SELECT dd.date_id,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_date_customer_painting
            SELECT dd.date_id,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_date_customer_painting_orders
            SELECT dd.date_id,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_date_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_date_customer_painting_status
            SELECT dd.date_id,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year;
            INSERT INTO [cubes_htttql].[dbo].cube_year
            SELECT dd.year, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_orders
            SELECT dd.year,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_status
            SELECT dd.year,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_painting
            SELECT dd.year,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_painting_orders
            SELECT dd.year,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_painting_status
            SELECT dd.year,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_customer
            SELECT dd.year,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_customer_orders
            SELECT dd.year,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_customer_status
            SELECT dd.year,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_customer_painting
            SELECT dd.year,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_customer_painting_orders
            SELECT dd.year,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_customer_painting_status
            SELECT dd.year,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter
            SELECT dd.quarter, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_orders
            SELECT dd.quarter,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_status
            SELECT dd.quarter,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_painting
            SELECT dd.quarter,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_painting_orders
            SELECT dd.quarter,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_painting_status
            SELECT dd.quarter,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_customer
            SELECT dd.quarter,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_customer_orders
            SELECT dd.quarter,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_customer_status
            SELECT dd.quarter,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_customer_painting
            SELECT dd.quarter,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_customer_painting_orders
            SELECT dd.quarter,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_customer_painting_status
            SELECT dd.quarter,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month;
            INSERT INTO [cubes_htttql].[dbo].cube_month
            SELECT dd.month, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_month_orders
            SELECT dd.month,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_status;
            INSERT INTO [cubes_htttql].[dbo].cube_month_status
            SELECT dd.month,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_month_painting
            SELECT dd.month,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_month_painting_orders
            SELECT dd.month,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_month_painting_status
            SELECT dd.month,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_month_customer
            SELECT dd.month,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_month_customer_orders
            SELECT dd.month,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_month_customer_status
            SELECT dd.month,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_month_customer_painting
            SELECT dd.month,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_month_customer_painting_orders
            SELECT dd.month,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_month_customer_painting_status
            SELECT dd.month,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day;
            INSERT INTO [cubes_htttql].[dbo].cube_day
            SELECT dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_day_orders
            SELECT dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_day_status
            SELECT dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_day_painting
            SELECT dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_day_painting_orders
            SELECT dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_day_painting_status
            SELECT dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_day_customer
            SELECT dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_day_customer_orders
            SELECT dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_day_customer_status
            SELECT dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_day_customer_painting
            SELECT dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_day_customer_painting_orders
            SELECT dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_day_customer_painting_status
            SELECT dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week;
            INSERT INTO [cubes_htttql].[dbo].cube_week
            SELECT dd.week, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_week_orders
            SELECT dd.week,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_status;
            INSERT INTO [cubes_htttql].[dbo].cube_week_status
            SELECT dd.week,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_week_painting
            SELECT dd.week,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_week_painting_orders
            SELECT dd.week,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_week_painting_status
            SELECT dd.week,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_week_customer
            SELECT dd.week,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_week_customer_orders
            SELECT dd.week,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_week_customer_status
            SELECT dd.week,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_week_customer_painting
            SELECT dd.week,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_week_customer_painting_orders
            SELECT dd.week,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_week_customer_painting_status
            SELECT dd.week,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day
            SELECT dd.month,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_orders
            SELECT dd.month,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_status
            SELECT dd.month,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_painting
            SELECT dd.month,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_painting_orders
            SELECT dd.month,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_painting_status
            SELECT dd.month,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_customer
            SELECT dd.month,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_customer_orders
            SELECT dd.month,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_customer_status
            SELECT dd.month,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_customer_painting
            SELECT dd.month,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_customer_painting_orders
            SELECT dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_month_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_month_day_customer_painting_status
            SELECT dd.month,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day
            SELECT dd.quarter,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_orders
            SELECT dd.quarter,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_status
            SELECT dd.quarter,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_painting
            SELECT dd.quarter,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_painting_orders
            SELECT dd.quarter,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_painting_status
            SELECT dd.quarter,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_customer
            SELECT dd.quarter,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_customer_orders
            SELECT dd.quarter,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_customer_status
            SELECT dd.quarter,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_customer_painting
            SELECT dd.quarter,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_customer_painting_orders
            SELECT dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_day_customer_painting_status
            SELECT dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month
            SELECT dd.quarter,dd.month, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_orders
            SELECT dd.quarter,dd.month,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_status
            SELECT dd.quarter,dd.month,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_painting
            SELECT dd.quarter,dd.month,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_painting_orders
            SELECT dd.quarter,dd.month,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_painting_status
            SELECT dd.quarter,dd.month,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_customer
            SELECT dd.quarter,dd.month,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_customer_orders
            SELECT dd.quarter,dd.month,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_customer_status
            SELECT dd.quarter,dd.month,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_customer_painting
            SELECT dd.quarter,dd.month,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_customer_painting_orders
            SELECT dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_customer_painting_status
            SELECT dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day
            SELECT dd.quarter,dd.month,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_orders
            SELECT dd.quarter,dd.month,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_status
            SELECT dd.quarter,dd.month,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_painting
            SELECT dd.quarter,dd.month,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_painting_orders
            SELECT dd.quarter,dd.month,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_painting_status
            SELECT dd.quarter,dd.month,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_customer
            SELECT dd.quarter,dd.month,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_customer_orders
            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_customer_status
            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting
            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting_orders
            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting_status
            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day
            SELECT dd.year,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_orders
            SELECT dd.year,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_status
            SELECT dd.year,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_painting
            SELECT dd.year,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_painting_orders
            SELECT dd.year,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_painting_status
            SELECT dd.year,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_customer
            SELECT dd.year,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_customer_orders
            SELECT dd.year,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_customer_status
            SELECT dd.year,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_customer_painting
            SELECT dd.year,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_customer_painting_orders
            SELECT dd.year,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_day_customer_painting_status
            SELECT dd.year,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month
            SELECT dd.year,dd.month, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_orders
            SELECT dd.year,dd.month,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_status
            SELECT dd.year,dd.month,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_painting
            SELECT dd.year,dd.month,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_painting_orders
            SELECT dd.year,dd.month,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_painting_status
            SELECT dd.year,dd.month,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_customer
            SELECT dd.year,dd.month,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_customer_orders
            SELECT dd.year,dd.month,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_customer_status
            SELECT dd.year,dd.month,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_customer_painting
            SELECT dd.year,dd.month,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_customer_painting_orders
            SELECT dd.year,dd.month,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_customer_painting_status
            SELECT dd.year,dd.month,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day
            SELECT dd.year,dd.month,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_orders
            SELECT dd.year,dd.month,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_status
            SELECT dd.year,dd.month,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_painting
            SELECT dd.year,dd.month,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_painting_orders
            SELECT dd.year,dd.month,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_painting_status
            SELECT dd.year,dd.month,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_customer
            SELECT dd.year,dd.month,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_customer_orders
            SELECT dd.year,dd.month,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_customer_status
            SELECT dd.year,dd.month,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_customer_painting
            SELECT dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_customer_painting_orders
            SELECT dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_month_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_month_day_customer_painting_status
            SELECT dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter
            SELECT dd.year,dd.quarter, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_orders
            SELECT dd.year,dd.quarter,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_status
            SELECT dd.year,dd.quarter,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_painting
            SELECT dd.year,dd.quarter,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_painting_orders
            SELECT dd.year,dd.quarter,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_painting_status
            SELECT dd.year,dd.quarter,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_customer
            SELECT dd.year,dd.quarter,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_customer_orders
            SELECT dd.year,dd.quarter,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_customer_status
            SELECT dd.year,dd.quarter,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_customer_painting
            SELECT dd.year,dd.quarter,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_customer_painting_orders
            SELECT dd.year,dd.quarter,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_customer_painting_status
            SELECT dd.year,dd.quarter,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day
            SELECT dd.year,dd.quarter,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_orders
            SELECT dd.year,dd.quarter,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_status
            SELECT dd.year,dd.quarter,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_painting
            SELECT dd.year,dd.quarter,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_painting_orders
            SELECT dd.year,dd.quarter,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_painting_status
            SELECT dd.year,dd.quarter,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_customer
            SELECT dd.year,dd.quarter,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_customer_orders
            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_customer_status
            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting
            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting_orders
            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting_status
            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month
            SELECT dd.year,dd.quarter,dd.month, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_orders
            SELECT dd.year,dd.quarter,dd.month,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_status
            SELECT dd.year,dd.quarter,dd.month,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_painting
            SELECT dd.year,dd.quarter,dd.month,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_painting_orders
            SELECT dd.year,dd.quarter,dd.month,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_painting_status
            SELECT dd.year,dd.quarter,dd.month,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_customer
            SELECT dd.year,dd.quarter,dd.month,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_customer_orders
            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_customer_status
            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting
            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting_orders
            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting_status
            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day
            SELECT dd.year,dd.quarter,dd.month,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_orders
            SELECT dd.year,dd.quarter,dd.month,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_status
            SELECT dd.year,dd.quarter,dd.month,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_painting
            SELECT dd.year,dd.quarter,dd.month,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_painting_orders
            SELECT dd.year,dd.quarter,dd.month,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_painting_status
            SELECT dd.year,dd.quarter,dd.month,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_customer
            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_orders
            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_status
            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting
            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting_orders
            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting_status
            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day
            SELECT dd.week,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_orders
            SELECT dd.week,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_status
            SELECT dd.week,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_painting
            SELECT dd.week,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_painting_orders
            SELECT dd.week,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_painting_status
            SELECT dd.week,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_customer
            SELECT dd.week,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_customer_orders
            SELECT dd.week,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_customer_status
            SELECT dd.week,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_customer_painting
            SELECT dd.week,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_customer_painting_orders
            SELECT dd.week,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_week_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_week_day_customer_painting_status
            SELECT dd.week,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week
            SELECT dd.year,dd.week, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_orders
            SELECT dd.year,dd.week,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_status
            SELECT dd.year,dd.week,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_painting
            SELECT dd.year,dd.week,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_painting_orders
            SELECT dd.year,dd.week,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_painting_status
            SELECT dd.year,dd.week,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_customer
            SELECT dd.year,dd.week,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_customer_orders
            SELECT dd.year,dd.week,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_customer_status
            SELECT dd.year,dd.week,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_customer_painting
            SELECT dd.year,dd.week,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_customer_painting_orders
            SELECT dd.year,dd.week,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_customer_painting_status
            SELECT dd.year,dd.week,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day
            SELECT dd.year,dd.week,dd.day, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_orders
            SELECT dd.year,dd.week,dd.day,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_status
            SELECT dd.year,dd.week,dd.day,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_painting
            SELECT dd.year,dd.week,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_painting_orders
            SELECT dd.year,dd.week,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_painting_status
            SELECT dd.year,dd.week,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dp.painting_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_customer;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_customer
            SELECT dd.year,dd.week,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_customer_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_customer_orders
            SELECT dd.year,dd.week,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_customer_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_customer_status
            SELECT dd.year,dd.week,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,do.status;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_customer_painting;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_customer_painting
            SELECT dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_customer_painting_orders;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_customer_painting_orders
            SELECT dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
                 
            

            TRUNCATE TABLE [cubes_htttql].[dbo].cube_year_week_day_customer_painting_status;
            INSERT INTO [cubes_htttql].[dbo].cube_year_week_day_customer_painting_status
            SELECT dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id,do.status;
     END;
GO
CREATE TRIGGER [dbo].[trgAfterInsert_fact_sales] 
ON [dwh_htttql].[dbo].[fact_sales]
AFTER INSERT
AS
BEGIN
    
    EXEC ProcessNewValuesFact_sales;
END;
GO
            SELECT dd.year,dd.month,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,do.status;
            go     
            

            SELECT dd.year,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id;
            go     
            

            SELECT dd.month, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month;
            go     
            

            SELECT dd.quarter,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dd.week,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,do.status;
            go     
            

            SELECT dd.quarter,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.week,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,do.status;
            go     
            

            SELECT dd.quarter,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dp.painting_id;
            go     
            

            SELECT dd.year,dd.quarter,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dp.painting_id,do.status;
            go     
            

            SELECT dd.quarter,dd.month,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id;
            go     
            

            SELECT dd.quarter,dd.month,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.date_id,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.year,dd.week,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,do.status;
            go     
            

            SELECT dd.year,dd.week,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dp.painting_id,do.status;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dp.painting_id;
            go     
            

            SELECT dd.quarter,dd.month,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id;
            go     
            

            SELECT dd.week,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dp.painting_id,do.status;
            go     
            

            SELECT dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,do.status;
            go     
            

            SELECT dd.week,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,do.orders_id;
            go     
            

            SELECT dd.week,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by do.orders_id;
            go     
            

            SELECT dd.quarter,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.date_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,do.status;
            go     
            

            SELECT dd.quarter,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dp.painting_id;
            go     
            

            SELECT dd.week,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.month,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.month,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,do.status;
            go     
            

            SELECT dd.year,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.year,dd.week,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.month,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,do.orders_id;
            go     
            

            SELECT dd.quarter,dd.month,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.week,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dp.painting_id;
            go     
            

            SELECT dd.week,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by do.status;
            go     
            

            SELECT dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.year,dd.month,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,do.status;
            go     
            

            SELECT dd.year,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year;
            go     
            

            SELECT dd.month,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,do.orders_id;
            go     
            

            SELECT dd.date_id,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.month,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,do.status;
            go     
            

            SELECT dd.month,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id;
            go     
            

            SELECT dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.month,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.date_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dp.painting_id;
            go     
            

            SELECT dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,do.status;
            go     
            

            SELECT dd.quarter,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.month,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,do.status;
            go     
            

            SELECT dd.week, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week;
            go     
            

            SELECT dd.quarter,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,do.status;
            go     
            

            SELECT dd.week,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.month,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dp.painting_id;
            go     
            

            SELECT dd.month,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,do.orders_id;
            go     
            

            SELECT dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.quarter,dd.month,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.month,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.week,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.month,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,do.status;
            go     
            

            SELECT dd.month,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dp.painting_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month;
            go     
            

            SELECT dd.year,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.quarter,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.week,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.quarter,dd.month,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dp.painting_id;
            go     
            

            SELECT dd.year,dd.week,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.quarter,dd.month, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month;
            go     
            

            SELECT dd.quarter,dd.month,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.month,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,do.orders_id;
            go     
            

            SELECT dd.year,dd.month,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.week,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dc.customer_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.month,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id;
            go     
            

            SELECT dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id;
            go     
            

            SELECT dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.month,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.quarter,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,do.orders_id;
            go     
            

            SELECT dd.year,dd.month, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.week,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.week,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dd.quarter,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.date_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id;
            go     
            

            SELECT dd.year,dd.week, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week;
            go     
            

            SELECT dd.quarter,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dp.painting_id;
            go     
            

            SELECT dd.quarter,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,do.status;
            go     
            

            SELECT dd.year,dd.month,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id;
            go     
            

            SELECT dd.year,dd.month,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id;
            go     
            

            SELECT dd.quarter,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id,do.status;
            go     
            

            SELECT dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dp.painting_id;
            go     
            

            SELECT dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.quarter,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,do.status;
            go     
            

            SELECT dd.year,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,do.status;
            go     
            

            SELECT dd.year,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.quarter,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dp.painting_id,do.status;
            go     
            

            SELECT dd.week,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.quarter,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dc.customer_id;
            go     
            

            SELECT dd.month,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.quarter,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dc.customer_id;
            go     
            

            SELECT dd.quarter, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter;
            go     
            

            SELECT dd.month,dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.quarter,dd.month,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,do.orders_id;
            go     
            

            SELECT dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.week,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dp.painting_id;
            go     
            

            SELECT dd.quarter,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.date_id,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.week,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id;
            go     
            

            SELECT dd.year,dd.quarter,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.month,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day;
            go     
            

            SELECT dd.year,dd.week,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.year,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,do.orders_id;
            go     
            

            SELECT dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dp.painting_id,do.status;
            go     
            

            SELECT dd.month,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.month,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id;
            go     
            

            SELECT dd.week,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day;
            go     
            

            SELECT dd.year,dd.quarter,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day;
            go     
            

            SELECT dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dp.painting_id;
            go     
            

            SELECT dd.week,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dp.painting_id;
            go     
            

            SELECT dd.year,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.year,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dp.painting_id;
            go     
            

            SELECT dd.year,dd.month,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.quarter,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.week,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day;
            go     
            

            SELECT dd.week,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,dp.painting_id;
            go     
            

            SELECT dd.date_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,do.orders_id;
            go     
            

            SELECT dd.month,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day;
            go     
            

            SELECT dd.year,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,do.orders_id;
            go     
            

            SELECT dd.week,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,do.status;
            go     
            

            SELECT dd.year,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dd.quarter,dd.month,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,do.status;
            go     
            

            SELECT dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.date_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.month,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.week,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.date_id,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id;
            go     
            

            SELECT dd.year,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day;
            go     
            

            SELECT dd.month,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,do.status;
            go     
            

            SELECT dd.year,dd.week,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dp.painting_id;
            go     
            

            SELECT dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.month,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dp.painting_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dp.painting_id;
            go     
            

            SELECT dd.quarter,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dp.painting_id,do.status;
            go     
            

            SELECT dd.week,dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT  sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            ;
            go     
            

            SELECT dd.quarter,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.day;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,do.status;
            go     
            

            SELECT dd.date_id,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.week,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,do.status;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.week,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id;
            go     
            

            SELECT dd.week,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dd.day,do.status;
            go     
            

            SELECT dd.quarter,dd.month,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dc.customer_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day;
            go     
            

            SELECT dd.year,dd.week,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id;
            go     
            

            SELECT dd.week,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_week_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.week,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_day_customer_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dd.day,dc.customer_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.month,dd.day,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_day_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dd.day,dc.customer_id,do.status;
            go     
            

            SELECT dd.month,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dc.customer_id;
            go     
            

            SELECT dd.year,dd.quarter,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.day,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,do.orders_id;
            go     
            

            SELECT dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id;
            go     
            

            SELECT dd.month,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_month_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.month,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.day,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dp.painting_id,do.status;
            go     
            

            SELECT dd.quarter,dd.month,dd.day,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_quarter_month_day_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.quarter,dd.month,dd.day,do.status;
            go     
            

            SELECT dd.year,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.day,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_customer_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dc.customer_id,dp.painting_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id;
            go     
            

            SELECT dd.date_id,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dp.painting_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_painting_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dp.painting_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dp.painting_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id;
            go     
            

            SELECT dd.year,dc.customer_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_customer
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dc.customer_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.day, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day;
            go     
            

            SELECT dd.date_id,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_date_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.date_id,dc.customer_id,do.status;
            go     
            

            SELECT dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_day_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.day,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.day,dp.painting_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dc.customer_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_customer_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dc.customer_id,do.status;
            go     
            

            SELECT dd.year,dd.month,dc.customer_id,dp.painting_id,do.status, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_month_customer_painting_status
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.month,dc.customer_id,dp.painting_id,do.status;
            go     
            

            SELECT dd.year,dd.week,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,do.orders_id;
            go     
            

            SELECT dd.year,dd.week,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_week_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.week,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.day,dc.customer_id,do.orders_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_day_customer_orders
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.day,dc.customer_id,do.orders_id;
            go     
            

            SELECT dd.year,dd.quarter,dd.month,dd.day,dp.painting_id, sum(fs.total_price) as total_price
            into [cubes_htttql].[dbo].cube_year_quarter_month_day_painting
            FROM [dwh_htttql].[dbo].[fact_sales] fs
            join dim_date dd on dd.date_id = fs.date_id
            join dim_customer dc on dc.customer_id = fs.customer_id
            join dim_painting dp on dp.painting_id = fs.painting_id
            join dim_orders do on do.orders_id = fs.orders_id
            where do.status_flag = 1 and dc.status_flag = 1 and dp.status_flag = 1
            group by dd.year,dd.quarter,dd.month,dd.day,dp.painting_id;
            go     
            
