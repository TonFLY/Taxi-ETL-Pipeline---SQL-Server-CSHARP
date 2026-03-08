USE [TaxiPipelineDB];
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'ops.usp_log_error') AND type = 'P')
    DROP PROCEDURE ops.usp_log_error;
GO

CREATE PROCEDURE ops.usp_log_error
    @batch_id           BIGINT,
    @step_name          NVARCHAR(200),
    @error_number       INT = NULL,
    @error_severity     INT = NULL,
    @error_state        INT = NULL,
    @error_procedure    NVARCHAR(200) = NULL,
    @error_line         INT = NULL,
    @error_message      NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ops.ExecutionError
    (
        batch_id,
        step_name,
        error_number,
        error_severity,
        error_state,
        error_procedure,
        error_line,
        error_message
    )
    VALUES
    (
        @batch_id,
        @step_name,
        @error_number,
        @error_severity,
        @error_state,
        @error_procedure,
        @error_line,
        @error_message
    );

    INSERT INTO ops.ExecutionLog (batch_id, step_name, step_status, finished_at, message)
    VALUES (@batch_id, @step_name, 'FAILED', SYSDATETIME(), @error_message);
END
GO

GO