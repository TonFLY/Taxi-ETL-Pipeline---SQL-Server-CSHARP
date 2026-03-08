using System.Data;
using Microsoft.Data.SqlClient;
using TaxiPipeline.Domain.Interfaces;

namespace TaxiPipeline.Infrastructure.Database;

public class StoredProcedureExecutor : IStoredProcedureExecutor
{
    private readonly SqlConnectionFactory _connectionFactory;

    public StoredProcedureExecutor(SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<int> ExecuteWithRowCountAsync(
        string procedureName,
        long batchId,
        string outputParameterName,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.CreateOpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(procedureName, connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandTimeout = 300
        };

        command.Parameters.AddWithValue("@batch_id", batchId);

        var outputParam = new SqlParameter(outputParameterName, SqlDbType.Int)
        {
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(outputParam);

        await command.ExecuteNonQueryAsync(cancellationToken);

        return outputParam.Value == DBNull.Value ? 0 : (int)outputParam.Value;
    }
}
