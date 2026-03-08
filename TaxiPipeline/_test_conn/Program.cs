using Microsoft.Data.SqlClient;

var cs = "Server=tonfly.cloud;User Id=sa;Password=Aa@@91684895;TrustServerCertificate=True;Connection Timeout=10;";
try
{
    using var conn = new SqlConnection(cs);
    conn.Open();
    Console.WriteLine($"CONEXAO_OK|{conn.ServerVersion}");
    using var cmd = new SqlCommand("SELECT DB_NAME()", conn);
    var db = cmd.ExecuteScalar();
    Console.WriteLine($"DATABASE_ATUAL|{db}");
    conn.Close();
}
catch (Exception ex)
{
    Console.WriteLine($"ERRO|{ex.Message}");
}
