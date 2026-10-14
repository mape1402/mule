namespace Microsoft.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Mule.EntityFrameworkCore;
using Mule.EntityFrameworkCore.Internal;

internal static class MuleDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseMuleModel(
        this DbContextOptionsBuilder optionsBuilder,
        Action<MuleEntityFrameworkOptions> configureModel = null)
    {
        var extension = new MuleDbContextOptionsExtension(configureModel);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        return optionsBuilder;
    }
}
