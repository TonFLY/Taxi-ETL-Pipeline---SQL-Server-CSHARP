USE [TaxiPipelineDB];
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.YellowTripClean') AND type = 'U')
BEGIN
    CREATE TABLE staging.YellowTripClean
    (
        clean_id                BIGINT IDENTITY(1,1)    NOT NULL,
        batch_id                BIGINT                  NOT NULL,
        raw_id                  BIGINT                  NOT NULL,

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

        cleaned_at              DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),
        is_duplicate            BIT                     NOT NULL DEFAULT 0,

        CONSTRAINT PK_YellowTripClean PRIMARY KEY CLUSTERED (clean_id),
        CONSTRAINT FK_YellowTripClean_Batch FOREIGN KEY (batch_id) REFERENCES ops.BatchControl (batch_id)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.YellowTripRejected') AND type = 'U')
BEGIN
    CREATE TABLE staging.YellowTripRejected
    (
        rejected_id             BIGINT IDENTITY(1,1)    NOT NULL,
        batch_id                BIGINT                  NOT NULL,
        raw_id                  BIGINT                  NOT NULL,

        vendor_id               NVARCHAR(50)            NULL,
        pickup_datetime         NVARCHAR(50)            NULL,
        dropoff_datetime        NVARCHAR(50)            NULL,
        passenger_count         NVARCHAR(50)            NULL,
        trip_distance           NVARCHAR(50)            NULL,
        rate_code               NVARCHAR(50)            NULL,
        store_and_fwd_flag      NVARCHAR(50)            NULL,
        pickup_location_id      NVARCHAR(50)            NULL,
        dropoff_location_id     NVARCHAR(50)            NULL,
        payment_type            NVARCHAR(50)            NULL,
        fare_amount             NVARCHAR(50)            NULL,
        extra                   NVARCHAR(50)            NULL,
        mta_tax                 NVARCHAR(50)            NULL,
        tip_amount              NVARCHAR(50)            NULL,
        tolls_amount            NVARCHAR(50)            NULL,
        improvement_surcharge   NVARCHAR(50)            NULL,
        total_amount            NVARCHAR(50)            NULL,
        congestion_surcharge    NVARCHAR(50)            NULL,
        airport_fee             NVARCHAR(50)            NULL,

        rejection_reason        NVARCHAR(MAX)           NOT NULL,
        rejection_rule          NVARCHAR(200)           NOT NULL,
        rejected_at             DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),

        CONSTRAINT PK_YellowTripRejected PRIMARY KEY CLUSTERED (rejected_id),
        CONSTRAINT FK_YellowTripRejected_Batch FOREIGN KEY (batch_id) REFERENCES ops.BatchControl (batch_id)
    );
END
GO

GO