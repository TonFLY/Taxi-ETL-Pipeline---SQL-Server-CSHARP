USE [TaxiPipelineDB];
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'ops.usp_start_batch') AND type = 'P')
    DROP PROCEDURE ops.usp_start_batch;
GO

CREATE PROCEDURE ops.usp_start_batch
    @source_file_name   NVARCHAR(500),
    @batch_id           BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        INSERT INTO ops.BatchControl
        (
            source_file_name,
            batch_status,
            started_at
        )
        VALUES
        (
            @source_file_name,
            'STARTED',
            SYSDATETIME()
        );

        SET @batch_id = SCOPE_IDENTITY();

        INSERT INTO ops.ExecutionLog (batch_id, step_name, step_status, message)
        VALUES (@batch_id, 'START_BATCH', 'SUCCESS', 'Batch started for file: ' + @source_file_name);

    END TRY
    BEGIN CATCH

        IF @batch_id IS NOT NULL
        BEGIN
            INSERT INTO ops.ExecutionError
            (batch_id, step_name, error_number, error_severity, error_state, error_procedure, error_line, error_message)
            VALUES
            (@batch_id, 'START_BATCH', ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE());
        END;

        THROW;
    END CATCH
END
GO

GO