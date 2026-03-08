USE [master];
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'TaxiPipelineDB')
BEGIN
    CREATE DATABASE [TaxiPipelineDB];
END
GO