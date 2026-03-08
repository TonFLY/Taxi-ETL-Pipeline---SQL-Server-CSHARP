USE [TaxiPipelineDB];
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.usp_clean_yellow_trip_data') AND type = 'P')
    DROP PROCEDURE staging.usp_clean_yellow_trip_data;
GO

CREATE PROCEDURE staging.usp_clean_yellow_trip_data
    @batch_id   BIGINT,
    @rows_cleaned INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @step_name NVARCHAR(200) = 'CLEAN_YELLOW_TRIP_DATA';
    DECLARE @log_id BIGINT;

    INSERT INTO ops.ExecutionLog (batch_id, step_name, step_status, message)
    VALUES (@batch_id, @step_name, 'RUNNING', 'Starting data cleaning and type conversion.');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY

        DELETE FROM staging.YellowTripClean WHERE batch_id = @batch_id;

        INSERT INTO staging.YellowTripClean
        (
            batch_id, raw_id,
            vendor_id, pickup_datetime, dropoff_datetime,
            passenger_count, trip_distance, rate_code,
            store_and_fwd_flag, pickup_location_id, dropoff_location_id,
            payment_type, fare_amount, extra, mta_tax,
            tip_amount, tolls_amount, improvement_surcharge,
            total_amount, congestion_surcharge, airport_fee,
            trip_duration_minutes, row_hash
        )
        SELECT
            r.batch_id,
            r.raw_id,

            TRY_CAST(r.vendor_id AS INT),
            TRY_CAST(r.pickup_datetime AS DATETIME2(3)),
            TRY_CAST(r.dropoff_datetime AS DATETIME2(3)),
            TRY_CAST(r.passenger_count AS SMALLINT),
            TRY_CAST(r.trip_distance AS DECIMAL(10,2)),
            TRY_CAST(r.rate_code AS SMALLINT),
            UPPER(LTRIM(RTRIM(NULLIF(r.store_and_fwd_flag, '')))),
            TRY_CAST(r.pickup_location_id AS INT),
            TRY_CAST(r.dropoff_location_id AS INT),
            TRY_CAST(r.payment_type AS SMALLINT),
            TRY_CAST(r.fare_amount AS DECIMAL(10,2)),
            TRY_CAST(r.extra AS DECIMAL(10,2)),
            TRY_CAST(r.mta_tax AS DECIMAL(10,2)),
            TRY_CAST(r.tip_amount AS DECIMAL(10,2)),
            TRY_CAST(r.tolls_amount AS DECIMAL(10,2)),
            TRY_CAST(r.improvement_surcharge AS DECIMAL(10,2)),
            TRY_CAST(r.total_amount AS DECIMAL(10,2)),
            TRY_CAST(r.congestion_surcharge AS DECIMAL(10,2)),
            TRY_CAST(r.airport_fee AS DECIMAL(10,2)),

            CASE
                WHEN TRY_CAST(r.pickup_datetime AS DATETIME2(3)) IS NOT NULL
                     AND TRY_CAST(r.dropoff_datetime AS DATETIME2(3)) IS NOT NULL
                THEN CAST(DATEDIFF(SECOND,
                        TRY_CAST(r.pickup_datetime AS DATETIME2(3)),
                        TRY_CAST(r.dropoff_datetime AS DATETIME2(3))
                     ) / 60.0 AS DECIMAL(10,2))
                ELSE NULL
            END,

            HASHBYTES('SHA2_256',
                CONCAT(
                    ISNULL(r.vendor_id, ''),         '|',
                    ISNULL(r.pickup_datetime, ''),   '|',
                    ISNULL(r.dropoff_datetime, ''),  '|',
                    ISNULL(r.passenger_count, ''),   '|',
                    ISNULL(r.trip_distance, ''),      '|',
                    ISNULL(r.pickup_location_id, ''), '|',
                    ISNULL(r.dropoff_location_id, ''),'|',
                    ISNULL(r.fare_amount, ''),        '|',
                    ISNULL(r.total_amount, '')
                )
            )
        FROM landing.YellowTripRaw r
        WHERE r.batch_id = @batch_id

          AND TRY_CAST(r.pickup_datetime AS DATETIME2(3)) IS NOT NULL
          AND TRY_CAST(r.dropoff_datetime AS DATETIME2(3)) IS NOT NULL;

        SET @rows_cleaned = @@ROWCOUNT;

        UPDATE ops.ExecutionLog
        SET step_status = 'SUCCESS',
            finished_at = SYSDATETIME(),
            rows_affected = @rows_cleaned,
            message = 'Cleaned ' + CAST(@rows_cleaned AS VARCHAR(20)) + ' records.'
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

        SET @rows_cleaned = 0;
        THROW;
    END CATCH
END
GO

GO