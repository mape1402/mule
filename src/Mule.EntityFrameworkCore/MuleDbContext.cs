namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;

public sealed class MuleDbContext : DbContext
{
    public MuleDbContext(DbContextOptions<MuleDbContext> options)
        : base(options)
    {
    }

    public DbSet<DurableAction> Actions => Set<DurableAction>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.UseMuleModel();
    }
}
