namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;

internal interface IMuleDbContextFactory<TDbContext>
    where TDbContext : DbContext
{
    TDbContext CreateDbContext();
}
