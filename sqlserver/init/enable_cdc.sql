USE banking_db;
GO

-- ==========================================================
-- 1. BẬT CDC CHO DATABASE
-- ==========================================================
IF EXISTS (
    SELECT 1 FROM sys.databases 
    WHERE name = 'banking_db' AND is_cdc_enabled = 0
)
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT N'Đã bật CDC thành công cho Database banking_db.';
END
ELSE
BEGIN
    PRINT N'Database banking_db đã được bật CDC trước đó.';
END
GO

-- ==========================================================
-- 2. BẬT CDC CHO TỪNG BẢNG
-- ==========================================================

-- 2.1. Bảng users
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'users' AND schema_id = SCHEMA_ID('dbo') AND is_tracked_by_cdc = 0)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'users',
        @role_name            = NULL,
        @supports_net_changes = 1;
    PRINT N'Đã bật CDC cho bảng dbo.users';
END
GO

-- 2.2. Bảng cards
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'cards' AND schema_id = SCHEMA_ID('dbo') AND is_tracked_by_cdc = 0)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'cards',
        @role_name            = NULL,
        @supports_net_changes = 1;
    PRINT N'Đã bật CDC cho bảng dbo.cards';
END
GO

-- 2.3. Bảng transactions
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'transactions' AND schema_id = SCHEMA_ID('dbo') AND is_tracked_by_cdc = 0)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'transactions',
        @role_name            = NULL,
        @supports_net_changes = 1;
    PRINT N'Đã bật CDC cho bảng dbo.transactions';
END
GO

-- 2.4. Bảng mcc_codes
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'mcc_codes' AND schema_id = SCHEMA_ID('dbo') AND is_tracked_by_cdc = 0)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'mcc_codes',
        @role_name            = NULL,
        @supports_net_changes = 1;
    PRINT N'Đã bật CDC cho bảng dbo.mcc_codes';
END
GO

-- ==========================================================
-- 3. KIỂM TRA LẠI KẾT QUẢ
-- ==========================================================
SELECT 
    s.name AS [schema_name],
    t.name AS [table_name],
    t.is_tracked_by_cdc AS [is_cdc_enabled]
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo';
GO