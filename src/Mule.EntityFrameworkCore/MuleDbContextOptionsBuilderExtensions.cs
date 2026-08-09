namespace Microsoft.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Mule.EntityFrameworkCore.Internal;

internal static class MuleDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseMuleModel(this DbContextOptionsBuilder optionsBuilder)
    {
        var extension = optionsBuilder.Options.FindExtension<MuleDbContextOptionsExtension>()
            ?? new MuleDbContextOptionsExtension();

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        return optionsBuilder;
    }
}
