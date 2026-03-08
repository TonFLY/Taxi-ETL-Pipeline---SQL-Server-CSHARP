namespace TaxiPipeline.Domain.Interfaces;

public interface IPipelineOrchestrator
{
    Task<bool> RunAsync(string filePath, CancellationToken cancellationToken = default);

    Task<int> RunAllAsync(CancellationToken cancellationToken = default);

    Task<bool> RunFromApiAsync(int year, int month, CancellationToken cancellationToken = default);
}
