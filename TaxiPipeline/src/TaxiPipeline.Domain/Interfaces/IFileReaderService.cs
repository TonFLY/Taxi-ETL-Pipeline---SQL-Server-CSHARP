using TaxiPipeline.Domain.Entities;

namespace TaxiPipeline.Domain.Interfaces;

public interface IFileReaderService
{
    Task<IReadOnlyList<TripRecord>> ReadFileAsync(string filePath, CancellationToken cancellationToken = default);
}
