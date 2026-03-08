USE [TaxiPipelineDB];
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'ops.BatchControl') AND type = 'U')
BEGIN
    CREATE TABLE ops.BatchControl
    (
        batch_id            BIGINT IDENTITY(1,1)    NOT NULL,
        batch_guid          UNIQUEIDENTIFIER        NOT NULL DEFAULT NEWID(),
        source_file_name    NVARCHAR(500)           NOT NULL,
        batch_status        VARCHAR(20)             NOT NULL DEFAULT 'STARTED',
        started_at          DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),
        finished_at         DATETIME2(3)            NULL,
        total_rows_read     INT                     NULL,
        total_rows_landed   INT                     NULL,
        total_rows_cleaned  INT                     NULL,
        total_rows_rejected INT                     NULL,
        total_rows_loaded   INT                     NULL,
        error_message       NVARCHAR(MAX)           NULL,
        created_by          NVARCHAR(128)           NOT NULL DEFAULT SYSTEM_USER,

        CONSTRAINT PK_BatchControl PRIMARY KEY CLUSTERED (batch_id),
        CONSTRAINT CK_BatchControl_Status CHECK (batch_status IN ('STARTED', 'COMPLETED', 'FAILED', 'REPROCESSING'))
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'ops.ExecutionLog') AND type = 'U')
BEGIN
    CREATE TABLE ops.ExecutionLog
    (
        log_id          BIGINT IDENTITY(1,1)    NOT NULL,
        batch_id        BIGINT                  NOT NULL,
        step_name       NVARCHAR(200)           NOT NULL,
        step_status     VARCHAR(20)             NOT NULL DEFAULT 'RUNNING',
        started_at      DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),
        finished_at     DATETIME2(3)            NULL,
        rows_affected   INT                     NULL,
        message         NVARCHAR(MAX)           NULL,

        CONSTRAINT PK_ExecutionLog PRIMARY KEY CLUSTERED (log_id),
        CONSTRAINT FK_ExecutionLog_Batch FOREIGN KEY (batch_id) REFERENCES ops.BatchControl (batch_id),
        CONSTRAINT CK_ExecutionLog_Status CHECK (step_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'WARNING'))
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'ops.ExecutionError') AND type = 'U')
BEGIN
    CREATE TABLE ops.ExecutionError
    (
        error_id            BIGINT IDENTITY(1,1)    NOT NULL,
        batch_id            BIGINT                  NOT NULL,
        step_name           NVARCHAR(200)           NULL,
        error_number        INT                     NULL,
        error_severity      INT                     NULL,
        error_state         INT                     NULL,
        error_procedure     NVARCHAR(200)           NULL,
        error_line          INT                     NULL,
        error_message       NVARCHAR(MAX)           NOT NULL,
        occurred_at         DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),

        CONSTRAINT PK_ExecutionError PRIMARY KEY CLUSTERED (error_id),
        CONSTRAINT FK_ExecutionError_Batch FOREIGN KEY (batch_id) REFERENCES ops.BatchControl (batch_id)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'ops.DataQualityIssue') AND type = 'U')
BEGIN
    CREATE TABLE ops.DataQualityIssue
    (
        issue_id            BIGINT IDENTITY(1,1)    NOT NULL,
        batch_id            BIGINT                  NOT NULL,
        table_name          NVARCHAR(256)           NOT NULL,
        column_name         NVARCHAR(128)           NULL,
        rule_name           NVARCHAR(200)           NOT NULL,
        severity            VARCHAR(20)             NOT NULL DEFAULT 'ERROR',
        issue_description   NVARCHAR(MAX)           NOT NULL,
        affected_rows       INT                     NULL,
        detected_at         DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),

        CONSTRAINT PK_DataQualityIssue PRIMARY KEY CLUSTERED (issue_id),
        CONSTRAINT FK_DataQualityIssue_Batch FOREIGN KEY (batch_id) REFERENCES ops.BatchControl (batch_id),
        CONSTRAINT CK_DataQualityIssue_Severity CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL'))
    );
END
GO

GO