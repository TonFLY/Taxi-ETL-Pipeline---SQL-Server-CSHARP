namespace TaxiPipeline.Domain.Interfaces;

public interface IApiDataService
{
    Task<string> DownloadTripDataAsync(int year, int month, CancellationToken cancellationToken = default);
}
