namespace Mule.EntityFrameworkCore.Tests;

public sealed class SqlServerFactAttribute : FactAttribute
{
    public SqlServerFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(SqlServerMuleTests.ConnectionStringEnvironmentVariable)))
            Skip = $"Set {SqlServerMuleTests.ConnectionStringEnvironmentVariable} to run SQL Server integration tests.";
    }
}
