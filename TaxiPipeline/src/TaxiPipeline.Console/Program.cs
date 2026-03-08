using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TaxiPipeline.Application.Orchestration;
using TaxiPipeline.Domain.Entities;
using TaxiPipeline.Domain.Interfaces;
using TaxiPipeline.Infrastructure.Api;
using TaxiPipeline.Infrastructure.Database;
using TaxiPipeline.Infrastructure.FileSystem;
using TaxiPipeline.Infrastructure.Logging;

namespace TaxiPipeline.Console;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        System.Console.OutputEncoding = System.Text.Encoding.UTF8;
        System.Console.WriteLine("================================================");
        System.Console.WriteLine("  TaxiPipeline ETL - NYC Yellow Taxi Data");
        System.Console.WriteLine("  Supports: CSV files, Parquet files, NYC API");
        System.Console.WriteLine("================================================");
        System.Console.WriteLine();

        try
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
                .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: false)
                .AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: false)
                .AddEnvironmentVariables(prefix: "TAXIPIPELINE_")
                .Build();

            var settings = new AppSettings();
            configuration.GetSection("Pipeline").Bind(settings);

            ValidateSettings(settings);

            var services = new ServiceCollection();
            ConfigureServices(services, settings);

            await using var serviceProvider = services.BuildServiceProvider();

            var orchestrator = serviceProvider.GetRequiredService<IPipelineOrchestrator>();

            using var cts = new CancellationTokenSource();
            System.Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
                System.Console.WriteLine("\nCancellation requested. Finishing current step...");
            };

            if (args.Length > 0 && !args[0].StartsWith("--") && File.Exists(args[0]))
            {
                var success = await orchestrator.RunAsync(args[0], cts.Token);
                return success ? 0 : 1;
            }

            if (args.Length > 0 && args[0].Equals("--api", StringComparison.OrdinalIgnoreCase))
            {
                var (year, month) = ParseApiArgs(args);
                System.Console.WriteLine($"  Mode: API download");
                System.Console.WriteLine($"  Period: {year}-{month:D2}");
                System.Console.WriteLine();

                var success = await orchestrator.RunFromApiAsync(year, month, cts.Token);
                return success ? 0 : 1;
            }

            if (args.Length > 0 && args[0].Equals("--help", StringComparison.OrdinalIgnoreCase))
            {
                ShowHelp();
                return 0;
            }

            var filesProcessed = await orchestrator.RunAllAsync(cts.Token);
            return filesProcessed > 0 ? 0 : 1;
        }
        catch (OperationCanceledException)
        {
            System.Console.WriteLine("Operation cancelled by user.");
            return 2;
        }
        catch (Exception ex)
        {
            System.Console.ForegroundColor = ConsoleColor.Red;
            System.Console.WriteLine($"FATAL ERROR: {ex.Message}");
            System.Console.ResetColor();
            System.Console.WriteLine(ex.StackTrace);
            return 1;
        }
    }

    private static void ConfigureServices(IServiceCollection services, AppSettings settings)
    {
        services.AddSingleton(settings);

        services.AddLogging(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Information);
            builder.AddConsole(options =>
            {
                options.TimestampFormat = "[yyyy-MM-dd HH:mm:ss] ";
            });
        });

        var connectionFactory = new SqlConnectionFactory(settings.ConnectionString);
        services.AddSingleton(connectionFactory);

        services.AddSingleton<IBatchService, BatchService>();
        services.AddSingleton<IRawLoadService, RawLoadService>();
        services.AddSingleton<IStoredProcedureExecutor, StoredProcedureExecutor>();
        services.AddSingleton<IFileReaderService, FileReaderResolver>();
        services.AddSingleton<IExecutionLogger, ExecutionLogger>();

        services.AddSingleton<HttpClient>(sp =>
        {
            var client = new HttpClient();
            client.Timeout = TimeSpan.FromMinutes(30);
            client.DefaultRequestHeaders.Add("User-Agent", "TaxiPipeline-ETL/1.0");
            return client;
        });
        services.AddSingleton<IApiDataService, TaxiApiService>();

        services.AddTransient<IPipelineOrchestrator, PipelineOrchestrator>();
    }

    private static void ValidateSettings(AppSettings settings)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(settings.ConnectionString))
            errors.Add("ConnectionString is required.");

        if (string.IsNullOrWhiteSpace(settings.InputDirectory))
            errors.Add("InputDirectory is required.");

        if (errors.Count > 0)
        {
            throw new InvalidOperationException(
                "Configuration validation failed:\n" +
                string.Join("\n", errors.Select(e => $"  - {e}")));
        }
    }

    private static (int year, int month) ParseApiArgs(string[] args)
    {
        var defaultDate = DateTime.Now.AddMonths(-2);
        int year = defaultDate.Year;
        int month = defaultDate.Month;

        if (args.Length >= 2 && int.TryParse(args[1], out int parsedYear))
            year = parsedYear;

        if (args.Length >= 3 && int.TryParse(args[2], out int parsedMonth))
            month = parsedMonth;

        return (year, month);
    }

    private static void ShowHelp()
    {
        System.Console.WriteLine("Usage:");
        System.Console.WriteLine();
        System.Console.WriteLine("  TaxiPipeline                          Process all CSV/Parquet files in InputDirectory");
        System.Console.WriteLine("  TaxiPipeline <file>                   Process a specific file (CSV or Parquet)");
        System.Console.WriteLine("  TaxiPipeline --api [year] [month]     Download from NYC TLC API and process");
        System.Console.WriteLine("  TaxiPipeline --help                   Show this help");
        System.Console.WriteLine();
        System.Console.WriteLine("Examples:");
        System.Console.WriteLine("  TaxiPipeline --api                    Download latest available month");
        System.Console.WriteLine("  TaxiPipeline --api 2024 6             Download June 2024 data");
        System.Console.WriteLine("  TaxiPipeline --api 2025 1             Download January 2025 data");
        System.Console.WriteLine("  TaxiPipeline data.parquet             Process a local Parquet file");
        System.Console.WriteLine("  TaxiPipeline data.csv                 Process a local CSV file");
        System.Console.WriteLine();
        System.Console.WriteLine("API data source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page");
    }
}
