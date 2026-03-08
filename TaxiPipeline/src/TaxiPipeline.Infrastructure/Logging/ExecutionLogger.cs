using System.Data;
using Microsoft.Data.SqlClient;
using TaxiPipeline.Domain.Interfaces;
using Microsoft.Extensions.Logging;

namespace TaxiPipeline.Infrastructure.Logging;

public class ExecutionLogger : IExecutionLogger
{
    private readonly ILogger<ExecutionLogger> _logger;
    private readonly Infrastructure.Database.SqlConnectionFactory _connectionFactory;

    public ExecutionLogger(ILogger<ExecutionLogger> logger, Infrastructure.Database.SqlConnectionFactory connectionFactory)
    {
        _logger = logger;
        _connectionFactory = connectionFactory;
    }

    public void LogInfo(string message)
    {
        _logger.LogInformation("[INFO] {Message}", message);
    }

    public void LogWarning(string message)
    {
        _logger.LogWarning("[WARN] {Message}", message);
    }

    public void LogError(string message, Exception? ex = null)
    {
        if (ex != null)
            _logger.LogError(ex, "[ERROR] {Message}", message);
        else
            _logger.LogError("[ERROR] {Message}", message);
    }

    public void LogStepStart(string stepName)
    {
        _logger.LogInformation("[STEP] {StepName} - STARTED", stepName);
    }

    public void LogStepEnd(string stepName, int rowsAffected)
    {
        _logger.LogInformation("[STEP] {StepName} - COMPLETED ({RowsAffected} rows)", stepName, rowsAffected);
    }

    public void LogStepError(string stepName, Exception ex)
    {
        _logger.LogError(ex, "[STEP] {StepName} - FAILED: {Message}", stepName, ex.Message);
    }

    public async Task LogErrorToDatabaseAsync(
        long batchId,
        string stepName,
        string errorMessage,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _connectionFactory.CreateOpenConnectionAsync(cancellationToken);
            await using var command = new SqlCommand("ops.usp_log_error", connection)
            {
                CommandType = CommandType.StoredProcedure,
                CommandTimeout = 15
            };

            command.Parameters.AddWithValue("@batch_id", batchId);
            command.Parameters.AddWithValue("@step_name", stepName);
            command.Parameters.AddWithValue("@error_message", errorMessage);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to log error to database for batch {BatchId}", batchId);
        }
    }
}
