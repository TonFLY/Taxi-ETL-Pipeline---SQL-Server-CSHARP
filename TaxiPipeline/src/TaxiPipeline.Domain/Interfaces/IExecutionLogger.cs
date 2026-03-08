namespace TaxiPipeline.Domain.Interfaces;

public interface IExecutionLogger
{
    void LogInfo(string message);
    void LogWarning(string message);
    void LogError(string message, Exception? ex = null);
    void LogStepStart(string stepName);
    void LogStepEnd(string stepName, int rowsAffected);
    void LogStepError(string stepName, Exception ex);

    Task LogErrorToDatabaseAsync(long batchId, string stepName, string errorMessage, CancellationToken cancellationToken = default);
}
