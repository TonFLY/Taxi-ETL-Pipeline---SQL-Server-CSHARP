using TaxiPipeline.Domain.Entities;
using TaxiPipeline.Domain.Interfaces;

namespace TaxiPipeline.Application.Orchestration;

public class PipelineOrchestrator : IPipelineOrchestrator
{
    private readonly IFileReaderService _fileReader;
    private readonly IBatchService _batchService;
    private readonly IRawLoadService _rawLoadService;
    private readonly IStoredProcedureExecutor _spExecutor;
    private readonly IExecutionLogger _logger;
    private readonly IApiDataService _apiService;
    private readonly AppSettings _settings;

    public PipelineOrchestrator(
        IFileReaderService fileReader,
        IBatchService batchService,
        IRawLoadService rawLoadService,
        IStoredProcedureExecutor spExecutor,
        IExecutionLogger logger,
        IApiDataService apiService,
        AppSettings settings)
    {
        _fileReader = fileReader;
        _batchService = batchService;
        _rawLoadService = rawLoadService;
        _spExecutor = spExecutor;
        _logger = logger;
        _apiService = apiService;
        _settings = settings;
    }

    public async Task<bool> RunAsync(string filePath, CancellationToken cancellationToken = default)
    {
        var fileName = Path.GetFileName(filePath);
        var context = new BatchContext { SourceFileName = fileName };

        _logger.LogInfo($"========================================");
        _logger.LogInfo($"Pipeline starting for file: {fileName}");
        _logger.LogInfo($"========================================");

        try
        {
            _logger.LogStepStart("StartBatch");
            context.BatchId = await _batchService.StartBatchAsync(fileName, cancellationToken);
            _logger.LogStepEnd("StartBatch", 0);
            _logger.LogInfo($"Batch ID: {context.BatchId}");

            _logger.LogStepStart("ReadFile");
            var records = await _fileReader.ReadFileAsync(filePath, cancellationToken);
            context.TotalRowsRead = records.Count;
            _logger.LogStepEnd("ReadFile", records.Count);

            if (records.Count == 0)
            {
                _logger.LogWarning("No records found in file. Finishing batch.");
                await _batchService.FinishBatchAsync(context, success: true, cancellationToken: cancellationToken);
                return true;
            }

            _logger.LogStepStart("InsertLanding");
            context.TotalRowsLanded = await _rawLoadService.LoadRawDataAsync(
                context.BatchId, records, cancellationToken);
            _logger.LogStepEnd("InsertLanding", context.TotalRowsLanded);

            _logger.LogStepStart("CleanData");
            context.TotalRowsCleaned = await _spExecutor.ExecuteWithRowCountAsync(
                "staging.usp_clean_yellow_trip_data",
                context.BatchId,
                "@rows_cleaned",
                cancellationToken);
            _logger.LogStepEnd("CleanData", context.TotalRowsCleaned);

            _logger.LogStepStart("RejectInvalid");
            context.TotalRowsRejected = await _spExecutor.ExecuteWithRowCountAsync(
                "staging.usp_reject_invalid_yellow_trip_data",
                context.BatchId,
                "@rows_rejected",
                cancellationToken);
            _logger.LogStepEnd("RejectInvalid", context.TotalRowsRejected);

            _logger.LogStepStart("Deduplicate");
            var rowsDeduplicated = await _spExecutor.ExecuteWithRowCountAsync(
                "staging.usp_deduplicate_yellow_trip_data",
                context.BatchId,
                "@rows_deduplicated",
                cancellationToken);
            _logger.LogStepEnd("Deduplicate", rowsDeduplicated);

            _logger.LogStepStart("LoadCore");
            context.TotalRowsLoaded = await _spExecutor.ExecuteWithRowCountAsync(
                "core.usp_load_trip",
                context.BatchId,
                "@rows_loaded",
                cancellationToken);
            _logger.LogStepEnd("LoadCore", context.TotalRowsLoaded);

            await _batchService.FinishBatchAsync(context, success: true, cancellationToken: cancellationToken);

            _logger.LogInfo($"========================================");
            _logger.LogInfo($"Pipeline COMPLETED successfully.");
            _logger.LogInfo(context.GetSummary());
            _logger.LogInfo($"========================================");

            if (_settings.ArchiveAfterProcessing)
            {
                ArchiveFile(filePath);
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Pipeline FAILED for file: {fileName}", ex);

            try
            {
                if (context.BatchId > 0)
                {
                    await _logger.LogErrorToDatabaseAsync(
                        context.BatchId, "PipelineOrchestrator", ex.Message, cancellationToken);
                    await _batchService.FinishBatchAsync(
                        context, success: false, errorMessage: ex.Message, cancellationToken: cancellationToken);
                }
            }
            catch (Exception innerEx)
            {
                _logger.LogError("Failed to log error to database.", innerEx);
            }

            return false;
        }
    }

    public async Task<int> RunAllAsync(CancellationToken cancellationToken = default)
    {
        var inputDir = _settings.InputDirectory;
        var pattern = _settings.FilePattern;

        if (!Directory.Exists(inputDir))
        {
            _logger.LogError($"Input directory does not exist: {inputDir}");
            return 0;
        }

        var patterns = pattern.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var files = patterns
            .SelectMany(p => Directory.GetFiles(inputDir, p))
            .Distinct()
            .OrderBy(f => f)
            .ToArray();

        if (files.Length == 0)
        {
            _logger.LogWarning($"No files matching '{pattern}' found in: {inputDir}");
            return 0;
        }

        _logger.LogInfo($"Found {files.Length} file(s) to process.");

        int successCount = 0;
        foreach (var file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var success = await RunAsync(file, cancellationToken);
            if (success) successCount++;
        }

        _logger.LogInfo($"Processing complete. {successCount}/{files.Length} files succeeded.");
        return successCount;
    }

    public async Task<bool> RunFromApiAsync(int year, int month, CancellationToken cancellationToken = default)
    {
        _logger.LogInfo($"========================================");
        _logger.LogInfo($"API MODE - Downloading data for {year}-{month:D2}");
        _logger.LogInfo($"========================================");

        try
        {
            _logger.LogStepStart("DownloadFromApi");
            var localPath = await _apiService.DownloadTripDataAsync(year, month, cancellationToken);
            _logger.LogStepEnd("DownloadFromApi", 0);
            _logger.LogInfo($"File downloaded to: {localPath}");

            if (_settings.MaxRecordsFromApi > 0)
            {
                _logger.LogInfo($"Max records limit: {_settings.MaxRecordsFromApi:N0}");
            }

            return await RunAsync(localPath, cancellationToken);
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError($"Failed to download data from API: {ex.Message}", ex);
            _logger.LogInfo("Possible causes:");
            _logger.LogInfo("  - The data for this month may not be published yet (NYC publishes with ~2 month delay)");
            _logger.LogInfo("  - Check your internet connection");
            _logger.LogInfo($"  - Verify the URL: {_settings.ApiBaseUrl}");
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError($"API pipeline failed: {ex.Message}", ex);
            return false;
        }
    }

    private void ArchiveFile(string filePath)
    {
        try
        {
            if (!Directory.Exists(_settings.ArchiveDirectory))
                Directory.CreateDirectory(_settings.ArchiveDirectory);

            var fileName = Path.GetFileName(filePath);
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var archiveName = $"{Path.GetFileNameWithoutExtension(fileName)}_{timestamp}{Path.GetExtension(fileName)}";
            var destPath = Path.Combine(_settings.ArchiveDirectory, archiveName);

            File.Move(filePath, destPath);
            _logger.LogInfo($"File archived to: {destPath}");
        }
        catch (Exception ex)
        {
            _logger.LogWarning($"Failed to archive file: {ex.Message}");
        }
    }
}
