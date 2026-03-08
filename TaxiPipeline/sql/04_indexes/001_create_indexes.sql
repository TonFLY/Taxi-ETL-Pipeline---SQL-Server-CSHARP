USE [TaxiPipelineDB];
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_BatchControl_Status' AND object_id = OBJECT_ID(N'ops.BatchControl'))
    CREATE NONCLUSTERED INDEX IX_BatchControl_Status
    ON ops.BatchControl (batch_status)
    INCLUDE (started_at, finished_at);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_ExecutionLog_BatchId' AND object_id = OBJECT_ID(N'ops.ExecutionLog'))
    CREATE NONCLUSTERED INDEX IX_ExecutionLog_BatchId
    ON ops.ExecutionLog (batch_id)
    INCLUDE (step_name, step_status);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_ExecutionError_BatchId' AND object_id = OBJECT_ID(N'ops.ExecutionError'))
    CREATE NONCLUSTERED INDEX IX_ExecutionError_BatchId
    ON ops.ExecutionError (batch_id)
    INCLUDE (step_name, error_message);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_DataQualityIssue_BatchId' AND object_id = OBJECT_ID(N'ops.DataQualityIssue'))
    CREATE NONCLUSTERED INDEX IX_DataQualityIssue_BatchId
    ON ops.DataQualityIssue (batch_id, severity)
    INCLUDE (rule_name, affected_rows);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_YellowTripRaw_BatchId' AND object_id = OBJECT_ID(N'landing.YellowTripRaw'))
    CREATE NONCLUSTERED INDEX IX_YellowTripRaw_BatchId
    ON landing.YellowTripRaw (batch_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_ImportFile_BatchId' AND object_id = OBJECT_ID(N'landing.ImportFile'))
    CREATE NONCLUSTERED INDEX IX_ImportFile_BatchId
    ON landing.ImportFile (batch_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_YellowTripClean_BatchId' AND object_id = OBJECT_ID(N'staging.YellowTripClean'))
    CREATE NONCLUSTERED INDEX IX_YellowTripClean_BatchId
    ON staging.YellowTripClean (batch_id)
    INCLUDE (is_duplicate);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_YellowTripClean_RowHash' AND object_id = OBJECT_ID(N'staging.YellowTripClean'))
    CREATE NONCLUSTERED INDEX IX_YellowTripClean_RowHash
    ON staging.YellowTripClean (row_hash);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_YellowTripClean_PickupDatetime' AND object_id = OBJECT_ID(N'staging.YellowTripClean'))
    CREATE NONCLUSTERED INDEX IX_YellowTripClean_PickupDatetime
    ON staging.YellowTripClean (pickup_datetime);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_YellowTripRejected_BatchId' AND object_id = OBJECT_ID(N'staging.YellowTripRejected'))
    CREATE NONCLUSTERED INDEX IX_YellowTripRejected_BatchId
    ON staging.YellowTripRejected (batch_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Trip_RowHash' AND object_id = OBJECT_ID(N'core.Trip'))
    CREATE UNIQUE NONCLUSTERED INDEX IX_Trip_RowHash
    ON core.Trip (row_hash);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Trip_PickupDatetime' AND object_id = OBJECT_ID(N'core.Trip'))
    CREATE NONCLUSTERED INDEX IX_Trip_PickupDatetime
    ON core.Trip (pickup_datetime)
    INCLUDE (dropoff_datetime, trip_distance, total_amount);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Trip_BatchId' AND object_id = OBJECT_ID(N'core.Trip'))
    CREATE NONCLUSTERED INDEX IX_Trip_BatchId
    ON core.Trip (batch_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Trip_PickupLocation' AND object_id = OBJECT_ID(N'core.Trip'))
    CREATE NONCLUSTERED INDEX IX_Trip_PickupLocation
    ON core.Trip (pickup_location_id)
    INCLUDE (pickup_datetime, trip_distance, fare_amount);
GO

GO