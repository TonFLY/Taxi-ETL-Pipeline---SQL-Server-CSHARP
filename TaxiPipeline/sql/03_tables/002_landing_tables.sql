USE [TaxiPipelineDB];
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'landing.ImportFile') AND type = 'U')
BEGIN
    CREATE TABLE landing.ImportFile
    (
        import_file_id      BIGINT IDENTITY(1,1)    NOT NULL,
        batch_id            BIGINT                  NOT NULL,
        file_name           NVARCHAR(500)           NOT NULL,
        file_path           NVARCHAR(1000)          NULL,
        file_size_bytes     BIGINT                  NULL,
        file_hash           VARCHAR(64)             NULL,
        row_count           INT                     NULL,
        imported_at         DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),

        CONSTRAINT PK_ImportFile PRIMARY KEY CLUSTERED (import_file_id),
        CONSTRAINT FK_ImportFile_Batch FOREIGN KEY (batch_id) REFERENCES ops.BatchControl (batch_id)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'landing.YellowTripRaw') AND type = 'U')
BEGIN
    CREATE TABLE landing.YellowTripRaw
    (
        raw_id                  BIGINT IDENTITY(1,1)    NOT NULL,
        batch_id                BIGINT                  NOT NULL,
        import_file_id          BIGINT                  NULL,
        source_line_number      INT                     NULL,

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

        ingested_at             DATETIME2(3)            NOT NULL DEFAULT SYSDATETIME(),

        CONSTRAINT PK_YellowTripRaw PRIMARY KEY CLUSTERED (raw_id),
        CONSTRAINT FK_YellowTripRaw_Batch FOREIGN KEY (batch_id) REFERENCES ops.BatchControl (batch_id)
    );
END
GO

GO