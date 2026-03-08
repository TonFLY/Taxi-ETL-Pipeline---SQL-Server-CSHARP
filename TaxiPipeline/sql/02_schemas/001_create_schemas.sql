USE [TaxiPipelineDB];
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'landing')
    EXEC('CREATE SCHEMA [landing] AUTHORIZATION [dbo]');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'staging')
    EXEC('CREATE SCHEMA [staging] AUTHORIZATION [dbo]');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'core')
    EXEC('CREATE SCHEMA [core] AUTHORIZATION [dbo]');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ops')
    EXEC('CREATE SCHEMA [ops] AUTHORIZATION [dbo]');
GO

GO