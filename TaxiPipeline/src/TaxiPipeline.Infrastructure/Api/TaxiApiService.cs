using TaxiPipeline.Domain.Entities;
using TaxiPipeline.Domain.Interfaces;

namespace TaxiPipeline.Infrastructure.Api;

public class TaxiApiService : IApiDataService
{
    private readonly HttpClient _httpClient;
    private readonly AppSettings _settings;

    public TaxiApiService(HttpClient httpClient, AppSettings settings)
    {
        _httpClient = httpClient;
        _settings = settings;
    }

    public async Task<string> DownloadTripDataAsync(
        int year, int month,
        CancellationToken cancellationToken = default)
    {
        ValidateYearMonth(year, month);

        var fileName = string.Format(_settings.ApiFileNamePattern, year, month);
        var url = _settings.ApiBaseUrl.TrimEnd('/') + "/" + fileName;

        var downloadDir = !string.IsNullOrWhiteSpace(_settings.DownloadDirectory)
            ? _settings.DownloadDirectory
            : _settings.InputDirectory;

        if (!Directory.Exists(downloadDir))
            Directory.CreateDirectory(downloadDir);

        var localPath = Path.Combine(downloadDir, fileName);

        if (File.Exists(localPath) && new FileInfo(localPath).Length > 0)
        {
            Console.WriteLine($"  File already exists locally: {localPath}");
            Console.WriteLine($"  Skipping download. Delete the file to force re-download.");
            return localPath;
        }

        Console.WriteLine($"  Downloading: {url}");
        Console.WriteLine($"  Destination: {localPath}");

        using var response = await _httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        var totalBytes = response.Content.Headers.ContentLength;
        if (totalBytes.HasValue)
        {
            Console.WriteLine($"  File size: {FormatBytes(totalBytes.Value)}");
        }

        var tempPath = localPath + ".downloading";

        try
        {
            await using var contentStream = await response.Content.ReadAsStreamAsync(cancellationToken);
            await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, 8192);

            var buffer = new byte[81920];
            long totalRead = 0;
            int bytesRead;
            var lastProgressUpdate = DateTime.UtcNow;

            while ((bytesRead = await contentStream.ReadAsync(buffer, cancellationToken)) > 0)
            {
                await fileStream.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
                totalRead += bytesRead;

                if ((DateTime.UtcNow - lastProgressUpdate).TotalSeconds >= 2)
                {
                    if (totalBytes.HasValue)
                    {
                        var percent = (double)totalRead / totalBytes.Value * 100;
                        Console.Write($"\r  Progress: {FormatBytes(totalRead)} / {FormatBytes(totalBytes.Value)} ({percent:F1}%)   ");
                    }
                    else
                    {
                        Console.Write($"\r  Downloaded: {FormatBytes(totalRead)}   ");
                    }
                    lastProgressUpdate = DateTime.UtcNow;
                }
            }

            Console.WriteLine($"\r  Download complete: {FormatBytes(totalRead)}                    ");
        }
        catch
        {
            if (File.Exists(tempPath))
                File.Delete(tempPath);
            throw;
        }

        if (File.Exists(localPath))
            File.Delete(localPath);
        File.Move(tempPath, localPath);

        Console.WriteLine($"  Saved to: {localPath}");
        return localPath;
    }

    private static void ValidateYearMonth(int year, int month)
    {
        if (year < 2009 || year > DateTime.Now.Year)
            throw new ArgumentOutOfRangeException(nameof(year),
                $"Year must be between 2009 and {DateTime.Now.Year}. Got: {year}");

        if (month < 1 || month > 12)
            throw new ArgumentOutOfRangeException(nameof(month),
                $"Month must be between 1 and 12. Got: {month}");
    }

    private static string FormatBytes(long bytes)
    {
        string[] suffixes = ["B", "KB", "MB", "GB"];
        int i = 0;
        double size = bytes;
        while (size >= 1024 && i < suffixes.Length - 1)
        {
            size /= 1024;
            i++;
        }
        return $"{size:F1} {suffixes[i]}";
    }
}
