USE [TaxiPipelineDB];
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'ops.usp_finish_batch') AND type = 'P')
    DROP PROCEDURE ops.usp_finish_batch;
GO

CREATE PROCEDURE ops.usp_finish_batch
    @batch_id               BIGINT,
    @batch_status           VARCHAR(20),
    @total_rows_read        INT = NULL,
    @total_rows_landed      INT = NULL,
    @total_rows_cleaned     INT = NULL,
    @total_rows_rejected    INT = NULL,
    @total_rows_loaded      INT = NULL,
    @error_message          NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        UPDATE ops.BatchControl
        SET
            batch_status        = @batch_status,
            finished_at         = SYSDATETIME(),
            total_rows_read     = ISNULL(@total_rows_read, total_rows_read),
            total_rows_landed   = ISNULL(@total_rows_landed, total_rows_landed),
            total_rows_cleaned  = ISNULL(@total_rows_cleaned, total_rows_cleaned),
            total_rows_rejected = ISNULL(@total_rows_rejected, total_rows_rejected),
            total_rows_loaded   = ISNULL(@total_rows_loaded, total_rows_loaded),
            error_message       = ISNULL(@error_message, error_message)
        WHERE batch_id = @batch_id;

        INSERT INTO ops.ExecutionLog (batch_id, step_name, step_status, finished_at, message)
        VALUES
        (
            @batch_id,
            'FINISH_BATCH',
            CASE WHEN @batch_status = 'COMPLETED' THEN 'SUCCESS' ELSE 'FAILED' END,
            SYSDATETIME(),
            'Batch finished with status: ' + @batch_status
        );

    END TRY
    BEGIN CATCH
        INSERT INTO ops.ExecutionError
        (batch_id, step_name, error_number, error_severity, error_state, error_procedure, error_line, error_message)
        VALUES
        (@batch_id, 'FINISH_BATCH', ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE());

        THROW;
    END CATCH
END
GO

GO