USE [TaxiPipelineDB];
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'core.usp_load_trip') AND type = 'P')
    DROP PROCEDURE core.usp_load_trip;
GO

CREATE PROCEDURE core.usp_load_trip
    @batch_id       BIGINT,
    @rows_loaded    INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @step_name NVARCHAR(200) = 'LOAD_TRIP';
    DECLARE @log_id BIGINT;

    INSERT INTO ops.ExecutionLog (batch_id, step_name, step_status, message)
    VALUES (@batch_id, @step_name, 'RUNNING', 'Starting final load to core.Trip.');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY

        DELETE FROM core.Trip WHERE batch_id = @batch_id;

        INSERT INTO core.Trip
        (
            batch_id, source_raw_id,
            vendor_id, pickup_datetime, dropoff_datetime,
            passenger_count, trip_distance, rate_code,
            store_and_fwd_flag, pickup_location_id, dropoff_location_id,
            payment_type, fare_amount, extra, mta_tax,
            tip_amount, tolls_amount, improvement_surcharge,
            total_amount, congestion_surcharge, airport_fee,
            trip_duration_minutes, row_hash
        )
        SELECT
            stg.batch_id,
            stg.raw_id,
            stg.vendor_id,
            stg.pickup_datetime,
            stg.dropoff_datetime,
            stg.passenger_count,
            stg.trip_distance,
            stg.rate_code,
            stg.store_and_fwd_flag,
            stg.pickup_location_id,
            stg.dropoff_location_id,
            stg.payment_type,
            stg.fare_amount,
            stg.extra,
            stg.mta_tax,
            stg.tip_amount,
            stg.tolls_amount,
            stg.improvement_surcharge,
            stg.total_amount,
            stg.congestion_surcharge,
            stg.airport_fee,
            stg.trip_duration_minutes,
            stg.row_hash
        FROM staging.YellowTripClean stg
        WHERE stg.batch_id = @batch_id
          AND stg.is_duplicate = 0

          AND NOT EXISTS (
              SELECT 1 FROM core.Trip t WHERE t.row_hash = stg.row_hash
          );

        SET @rows_loaded = @@ROWCOUNT;

        UPDATE ops.ExecutionLog
        SET step_status = 'SUCCESS',
            finished_at = SYSDATETIME(),
            rows_affected = @rows_loaded,
            message = 'Loaded ' + CAST(@rows_loaded AS VARCHAR(20)) + ' records into core.Trip.'
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

        SET @rows_loaded = 0;
        THROW;
    END CATCH
END
GO

GO