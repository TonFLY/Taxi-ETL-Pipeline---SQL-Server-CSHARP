USE [TaxiPipelineDB];
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'landing.usp_insert_yellow_trip_raw') AND type = 'P')
    DROP PROCEDURE landing.usp_insert_yellow_trip_raw;
GO

CREATE PROCEDURE landing.usp_insert_yellow_trip_raw
    @batch_id               BIGINT,
    @import_file_id         BIGINT = NULL,
    @source_line_number     INT = NULL,
    @vendor_id              NVARCHAR(50) = NULL,
    @pickup_datetime        NVARCHAR(50) = NULL,
    @dropoff_datetime       NVARCHAR(50) = NULL,
    @passenger_count        NVARCHAR(50) = NULL,
    @trip_distance          NVARCHAR(50) = NULL,
    @rate_code              NVARCHAR(50) = NULL,
    @store_and_fwd_flag     NVARCHAR(50) = NULL,
    @pickup_location_id     NVARCHAR(50) = NULL,
    @dropoff_location_id    NVARCHAR(50) = NULL,
    @payment_type           NVARCHAR(50) = NULL,
    @fare_amount            NVARCHAR(50) = NULL,
    @extra                  NVARCHAR(50) = NULL,
    @mta_tax                NVARCHAR(50) = NULL,
    @tip_amount             NVARCHAR(50) = NULL,
    @tolls_amount           NVARCHAR(50) = NULL,
    @improvement_surcharge  NVARCHAR(50) = NULL,
    @total_amount           NVARCHAR(50) = NULL,
    @congestion_surcharge   NVARCHAR(50) = NULL,
    @airport_fee            NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO landing.YellowTripRaw
    (
        batch_id, import_file_id, source_line_number,
        vendor_id, pickup_datetime, dropoff_datetime,
        passenger_count, trip_distance, rate_code,
        store_and_fwd_flag, pickup_location_id, dropoff_location_id,
        payment_type, fare_amount, extra, mta_tax,
        tip_amount, tolls_amount, improvement_surcharge,
        total_amount, congestion_surcharge, airport_fee
    )
    VALUES
    (
        @batch_id, @import_file_id, @source_line_number,
        NULLIF(LTRIM(RTRIM(@vendor_id)), ''),
        NULLIF(LTRIM(RTRIM(@pickup_datetime)), ''),
        NULLIF(LTRIM(RTRIM(@dropoff_datetime)), ''),
        NULLIF(LTRIM(RTRIM(@passenger_count)), ''),
        NULLIF(LTRIM(RTRIM(@trip_distance)), ''),
        NULLIF(LTRIM(RTRIM(@rate_code)), ''),
        NULLIF(LTRIM(RTRIM(@store_and_fwd_flag)), ''),
        NULLIF(LTRIM(RTRIM(@pickup_location_id)), ''),
        NULLIF(LTRIM(RTRIM(@dropoff_location_id)), ''),
        NULLIF(LTRIM(RTRIM(@payment_type)), ''),
        NULLIF(LTRIM(RTRIM(@fare_amount)), ''),
        NULLIF(LTRIM(RTRIM(@extra)), ''),
        NULLIF(LTRIM(RTRIM(@mta_tax)), ''),
        NULLIF(LTRIM(RTRIM(@tip_amount)), ''),
        NULLIF(LTRIM(RTRIM(@tolls_amount)), ''),
        NULLIF(LTRIM(RTRIM(@improvement_surcharge)), ''),
        NULLIF(LTRIM(RTRIM(@total_amount)), ''),
        NULLIF(LTRIM(RTRIM(@congestion_surcharge)), ''),
        NULLIF(LTRIM(RTRIM(@airport_fee)), '')
    );
END
GO

GO