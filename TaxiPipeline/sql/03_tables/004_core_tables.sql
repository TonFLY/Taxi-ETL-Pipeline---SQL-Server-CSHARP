USE [TaxiPipelineDB];
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'core.Trip') AND type = 'U')
BEGIN
    CREATE TABLE core.Trip
    (
        trip_id                 BIGINT IDENTITY(1,1)    NOT NULL,
        batch_id                BIGINT                  NOT NULL,
        source_raw_id           BIGINT                  NOT NULL,

        vendor_id               INT                     NULL,
        pickup_datetime         DATETIME2(3)            NOT NULL,
        dropoff_datetime        DATETIME2(3)            NOT NULL,
        passenger_count         SMALLINT                NULL,
        trip_distance           DECIMAL(10,2)           NULL,
        rate_code               SMALLINT                NULL,
        store_and_fwd_flag      CHAR(1)                 NULL,
        pickup_location_id      INT                     NULL,
        dropoff_location_id     INT                     NULL,
        payment_type            SMALLINT                NULL,
        fare_amount             DECIMAL(10,2)           NULL,
        extra                   DECIMAL(10,2)           NULL,
        mta_tax                 DECIMAL(10,2)           NULL,
        tip_amount              DECIMAL(10,2)           NULL,
        tolls_amount            DECIMAL(10,2)           NULL,
        improvement_surcharge   DECIMAL(10,2)           NULL,
        total_amount            DECIMAL(10,2)           NULL,
        congestion_surcharge    DECIMAL(10,2)           NULL,
        airport_fee             DECIMAL(10,2)           NULL,

        trip_duration_minutes   DECIMAL(10,2)           NULL,
        row_hash                VARBINARY(32)           NOT NULL,

        loaded_at               DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),

        CONSTRAINT PK_Trip PRIMARY KEY CLUSTERED (trip_id),
        CONSTRAINT FK_Trip_Batch FOREIGN KEY (batch_id) REFERENCES ops.BatchControl (batch_id)
    );
END
GO

GO