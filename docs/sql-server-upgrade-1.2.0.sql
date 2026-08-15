-- Mule durable concurrency schema upgrade for SQL Server.
-- Apply this before deploying the version that introduces lanes and latency tracking.

IF COL_LENGTH('dbo.MuleActions', 'Lane') IS NULL
BEGIN
    ALTER TABLE dbo.MuleActions ADD Lane nvarchar(128) NULL;
END

UPDATE dbo.MuleActions
SET Lane = 'default'
WHERE Lane IS NULL OR LTRIM(RTRIM(Lane)) = '';

ALTER TABLE dbo.MuleActions ALTER COLUMN Lane nvarchar(128) NOT NULL;

IF COL_LENGTH('dbo.MuleActions', 'StartedOnUtc') IS NULL
BEGIN
    ALTER TABLE dbo.MuleActions ADD StartedOnUtc datetimeoffset NULL;
END

IF COL_LENGTH('dbo.MuleActions', 'TerminalOnUtc') IS NULL
BEGIN
    ALTER TABLE dbo.MuleActions ADD TerminalOnUtc datetimeoffset NULL;
END

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_MuleActions_Key_DeduplicationKey'
      AND object_id = OBJECT_ID('dbo.MuleActions')
)
BEGIN
    DROP INDEX IX_MuleActions_Key_DeduplicationKey ON dbo.MuleActions;
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_MuleActions_Key_DeduplicationKey'
      AND object_id = OBJECT_ID('dbo.MuleActions')
)
BEGIN
    CREATE UNIQUE INDEX UX_MuleActions_Key_DeduplicationKey
    ON dbo.MuleActions ([Key], DeduplicationKey)
    WHERE DeduplicationKey IS NOT NULL;
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_MuleActions_Lane_Status_NextAttemptOnUtc_CreatedOnUtc'
      AND object_id = OBJECT_ID('dbo.MuleActions')
)
BEGIN
    CREATE INDEX IX_MuleActions_Lane_Status_NextAttemptOnUtc_CreatedOnUtc
    ON dbo.MuleActions (Lane, Status, NextAttemptOnUtc, CreatedOnUtc);
END
