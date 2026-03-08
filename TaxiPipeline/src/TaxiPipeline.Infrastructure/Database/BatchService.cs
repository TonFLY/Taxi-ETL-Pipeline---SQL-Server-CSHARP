using System.Data;
using Microsoft.Data.SqlClient;
using TaxiPipeline.Domain.Entities;
using TaxiPipeline.Domain.Interfaces;

namespace TaxiPipeline.Infrastructure.Database;

public class BatchService : IBatchService
{
    private readonly SqlConnectionFactory _connectionFactory;

    public BatchService(SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<long> StartBatchAsync(string sourceFileName, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.CreateOpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand("ops.usp_start_batch", connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandTimeout = 30
        };

        command.Parameters.AddWithValue("@source_file_name", sourceFileName);

        var batchIdParam = new SqlParameter("@batch_id", SqlDbType.BigInt)
        {
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(batchIdParam);

        await command.ExecuteNonQueryAsync(cancellationToken);

        return (long)batchIdParam.Value;
    }

    public async Task FinishBatchAsync(
        BatchContext context,
        bool success,
        string? errorMessage = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.CreateOpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand("ops.usp_finish_batch", connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandTimeout = 30
        };

        command.Parameters.AddWithValue("@batch_id", context.BatchId);
        command.Parameters.AddWithValue("@batch_status", success ? "COMPLETED" : "FAILED");
        command.Parameters.AddWithValue("@total_rows_read", context.TotalRowsRead);
        command.Parameters.AddWithValue("@total_rows_landed", context.TotalRowsLanded);
        command.Parameters.AddWithValue("@total_rows_cleaned", context.TotalRowsCleaned);
        command.Parameters.AddWithValue("@total_rows_rejected", context.TotalRowsRejected);
        command.Parameters.AddWithValue("@total_rows_loaded", context.TotalRowsLoaded);
        command.Parameters.AddWithValue("@error_message",
            (object?)errorMessage ?? DBNull.Value);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
