USE [TaxiPipelineDB];
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.usp_deduplicate_yellow_trip_data') AND type = 'P')
    DROP PROCEDURE staging.usp_deduplicate_yellow_trip_data;
GO

CREATE PROCEDURE staging.usp_deduplicate_yellow_trip_data
    @batch_id               BIGINT,
    @rows_deduplicated      INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @step_name NVARCHAR(200) = 'DEDUPLICATE_YELLOW_TRIP_DATA';
    DECLARE @log_id BIGINT;
    DECLARE @within_batch_dupes INT = 0;
    DECLARE @cross_batch_dupes INT = 0;

    INSERT INTO ops.ExecutionLog (batch_id, step_name, step_status, message)
    VALUES (@batch_id, @step_name, 'RUNNING', 'Starting deduplication.');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY

        UPDATE staging.YellowTripClean
        SET is_duplicate = 0
        WHERE batch_id = @batch_id;

        ;WITH DuplicatesWithinBatch AS
        (
            SELECT
                clean_id,
                ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY clean_id ASC) AS rn
            FROM staging.YellowTripClean
            WHERE batch_id = @batch_id
              AND is_duplicate = 0
        )
        UPDATE staging.YellowTripClean
        SET is_duplicate = 1
        WHERE clean_id IN (
            SELECT clean_id FROM DuplicatesWithinBatch WHERE rn > 1
        );

        SET @within_batch_dupes = @@ROWCOUNT;

        UPDATE stg
        SET stg.is_duplicate = 1
        FROM staging.YellowTripClean stg
        INNER JOIN core.Trip t ON stg.row_hash = t.row_hash
        WHERE stg.batch_id = @batch_id
          AND stg.is_duplicate = 0;

        SET @cross_batch_dupes = @@ROWCOUNT;

        SET @rows_deduplicated = @within_batch_dupes + @cross_batch_dupes;

        IF @rows_deduplicated > 0
        BEGIN
            INSERT INTO ops.DataQualityIssue
            (batch_id, table_name, rule_name, severity, issue_description, affected_rows)
            VALUES
            (
                @batch_id,
                'staging.YellowTripClean',
                'DEDUP',
                'WARNING',
                'Duplicates found: ' + CAST(@within_batch_dupes AS VARCHAR(20))
                    + ' within batch, ' + CAST(@cross_batch_dupes AS VARCHAR(20)) + ' cross-batch.',
                @rows_deduplicated
            );
        END

        UPDATE ops.ExecutionLog
        SET step_status = 'SUCCESS',
            finished_at = SYSDATETIME(),
            rows_affected = @rows_deduplicated,
            message = 'Marked ' + CAST(@rows_deduplicated AS VARCHAR(20)) + ' duplicates ('
                     + CAST(@within_batch_dupes AS VARCHAR(20)) + ' within-batch, '
                     + CAST(@cross_batch_dupes AS VARCHAR(20)) + ' cross-batch).'
        WHERE log_id = @log_id;

    END TRY
    BEGIN CATCH
        UPDATE ops.ExecutionLog
        SET step_status = 'FAILED', finished_at = SYSDATETIME(), message = ERROR_MESSAGE()
        WHERE log_id = @log_id;

        INSERT INTO ops.ExecutionError
        (batch_id, step_name, error_number, error_severity, error_state, error_procedure, error_line, error_message)
        VALUES
        (@batch_id, @step_name, ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE());

        SET @rows_deduplicated = 0;
        THROW;
    END CATCH
END
GO

GO