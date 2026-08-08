namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Mule;
using Mule.Diagnostics;
using Mule.EntityFrameworkCore;

public static class EntityFrameworkMuleServiceCollectionExtensions
{
    public static IServiceCollection UseEntityFrameworkMule(
        this IServiceCollection services,
        Action<DbContextOptionsBuilder> configure)
    {
        if (services == null)
            throw new ArgumentNullException(nameof(services));

        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        services.AddDbContext<MuleDbContext>(configure);
        services.TryAddScoped<IMuleStorage, EntityFrameworkMuleStorage>();
        services.TryAddScoped<IMuleDiagnostics, EntityFrameworkMuleDiagnostics>();

        return services;
    }
}
