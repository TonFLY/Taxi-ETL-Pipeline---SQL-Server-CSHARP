USE [TaxiPipelineDB];
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.usp_reject_invalid_yellow_trip_data') AND type = 'P')
    DROP PROCEDURE staging.usp_reject_invalid_yellow_trip_data;
GO

CREATE PROCEDURE staging.usp_reject_invalid_yellow_trip_data
    @batch_id       BIGINT,
    @rows_rejected  INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @step_name NVARCHAR(200) = 'REJECT_INVALID_YELLOW_TRIP_DATA';
    DECLARE @log_id BIGINT;
    DECLARE @rule_count INT;

    INSERT INTO ops.ExecutionLog (batch_id, step_name, step_status, message)
    VALUES (@batch_id, @step_name, 'RUNNING', 'Starting validation and rejection.');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY

        DELETE FROM staging.YellowTripRejected WHERE batch_id = @batch_id;

        INSERT INTO staging.YellowTripRejected
        (
            batch_id, raw_id,
            vendor_id, pickup_datetime, dropoff_datetime,
            passenger_count, trip_distance, rate_code,
            store_and_fwd_flag, pickup_location_id, dropoff_location_id,
            payment_type, fare_amount, extra, mta_tax,
            tip_amount, tolls_amount, improvement_surcharge,
            total_amount, congestion_surcharge, airport_fee,
            rejection_reason, rejection_rule
        )
        SELECT
            r.batch_id, r.raw_id,
            r.vendor_id, r.pickup_datetime, r.dropoff_datetime,
            r.passenger_count, r.trip_distance, r.rate_code,
            r.store_and_fwd_flag, r.pickup_location_id, r.dropoff_location_id,
            r.payment_type, r.fare_amount, r.extra, r.mta_tax,
            r.tip_amount, r.tolls_amount, r.improvement_surcharge,
            r.total_amount, r.congestion_surcharge, r.airport_fee,

            STUFF(
                CASE WHEN TRY_CAST(r.pickup_datetime AS DATETIME2) IS NULL THEN '; R001: pickup_datetime is NULL or invalid' ELSE '' END +
                CASE WHEN TRY_CAST(r.dropoff_datetime AS DATETIME2) IS NULL THEN '; R002: dropoff_datetime is NULL or invalid' ELSE '' END +
                CASE WHEN TRY_CAST(r.dropoff_datetime AS DATETIME2) <= TRY_CAST(r.pickup_datetime AS DATETIME2) THEN '; R003: dropoff not after pickup' ELSE '' END +
                CASE WHEN TRY_CAST(r.trip_distance AS DECIMAL(10,2)) < 0 THEN '; R004: negative trip_distance' ELSE '' END +
                CASE WHEN TRY_CAST(r.fare_amount AS DECIMAL(10,2)) < 0 THEN '; R005: negative fare_amount' ELSE '' END +
                CASE WHEN TRY_CAST(r.total_amount AS DECIMAL(10,2)) < 0 THEN '; R006: negative total_amount' ELSE '' END +
                CASE WHEN TRY_CAST(r.passenger_count AS SMALLINT) NOT BETWEEN 0 AND 9 THEN '; R007: passenger_count out of range' ELSE '' END +
                CASE WHEN DATEDIFF(MINUTE, TRY_CAST(r.pickup_datetime AS DATETIME2), TRY_CAST(r.dropoff_datetime AS DATETIME2)) > 720 THEN '; R008: trip duration exceeds 12 hours' ELSE '' END +
                CASE WHEN TRY_CAST(r.total_amount AS DECIMAL(10,2)) < TRY_CAST(r.fare_amount AS DECIMAL(10,2)) THEN '; R009: total_amount less than fare_amount' ELSE '' END +
                CASE WHEN TRY_CAST(r.pickup_location_id AS INT) NOT BETWEEN 1 AND 265 THEN '; R010: invalid pickup_location_id' ELSE '' END +
                CASE WHEN TRY_CAST(r.dropoff_location_id AS INT) NOT BETWEEN 1 AND 265 THEN '; R011: invalid dropoff_location_id' ELSE '' END
            , 1, 2, ''),

            CASE
                WHEN TRY_CAST(r.pickup_datetime AS DATETIME2) IS NULL THEN 'R001'
                WHEN TRY_CAST(r.dropoff_datetime AS DATETIME2) IS NULL THEN 'R002'
                WHEN TRY_CAST(r.dropoff_datetime AS DATETIME2) <= TRY_CAST(r.pickup_datetime AS DATETIME2) THEN 'R003'
                WHEN TRY_CAST(r.trip_distance AS DECIMAL(10,2)) < 0 THEN 'R004'
                WHEN TRY_CAST(r.fare_amount AS DECIMAL(10,2)) < 0 THEN 'R005'
                WHEN TRY_CAST(r.total_amount AS DECIMAL(10,2)) < 0 THEN 'R006'
                WHEN TRY_CAST(r.passenger_count AS SMALLINT) NOT BETWEEN 0 AND 9 THEN 'R007'
                WHEN DATEDIFF(MINUTE, TRY_CAST(r.pickup_datetime AS DATETIME2), TRY_CAST(r.dropoff_datetime AS DATETIME2)) > 720 THEN 'R008'
                WHEN TRY_CAST(r.total_amount AS DECIMAL(10,2)) < TRY_CAST(r.fare_amount AS DECIMAL(10,2)) THEN 'R009'
                WHEN TRY_CAST(r.pickup_location_id AS INT) NOT BETWEEN 1 AND 265 THEN 'R010'
                WHEN TRY_CAST(r.dropoff_location_id AS INT) NOT BETWEEN 1 AND 265 THEN 'R011'
                ELSE 'R000'
            END
        FROM landing.YellowTripRaw r
        WHERE r.batch_id = @batch_id
          AND (
                TRY_CAST(r.pickup_datetime AS DATETIME2) IS NULL
                OR TRY_CAST(r.dropoff_datetime AS DATETIME2) IS NULL
                OR TRY_CAST(r.dropoff_datetime AS DATETIME2) <= TRY_CAST(r.pickup_datetime AS DATETIME2)
                OR TRY_CAST(r.trip_distance AS DECIMAL(10,2)) < 0
                OR TRY_CAST(r.fare_amount AS DECIMAL(10,2)) < 0
                OR TRY_CAST(r.total_amount AS DECIMAL(10,2)) < 0
                OR TRY_CAST(r.passenger_count AS SMALLINT) NOT BETWEEN 0 AND 9
                OR DATEDIFF(MINUTE, TRY_CAST(r.pickup_datetime AS DATETIME2), TRY_CAST(r.dropoff_datetime AS DATETIME2)) > 720
                OR TRY_CAST(r.total_amount AS DECIMAL(10,2)) < TRY_CAST(r.fare_amount AS DECIMAL(10,2))
                OR TRY_CAST(r.pickup_location_id AS INT) NOT BETWEEN 1 AND 265
                OR TRY_CAST(r.dropoff_location_id AS INT) NOT BETWEEN 1 AND 265
          );

        SET @rows_rejected = @@ROWCOUNT;

        DELETE c
        FROM staging.YellowTripClean c
        INNER JOIN staging.YellowTripRejected rej ON c.raw_id = rej.raw_id AND c.batch_id = rej.batch_id
        WHERE c.batch_id = @batch_id;

        INSERT INTO ops.DataQualityIssue (batch_id, table_name, rule_name, severity, issue_description, affected_rows)
        SELECT
            @batch_id,
            'landing.YellowTripRaw',
            rejection_rule,
            'ERROR',
            'Validation rule failed: ' + rejection_rule,
            COUNT(*)
        FROM staging.YellowTripRejected
        WHERE batch_id = @batch_id
        GROUP BY rejection_rule;

        UPDATE ops.ExecutionLog
        SET step_status = 'SUCCESS',
            finished_at = SYSDATETIME(),
            rows_affected = @rows_rejected,
            message = 'Rejected ' + CAST(@rows_rejected AS VARCHAR(20)) + ' records.'
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

        SET @rows_rejected = 0;
        THROW;
    END CATCH
END
GO

GO