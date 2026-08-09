namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Mule;
using Mule.Configuration;
using Mule.Diagnostics;
using Mule.EntityFrameworkCore;

public static class EntityFrameworkMuleServiceCollectionExtensions
{
    public static IMuleRegistrationBuilder UseEntityFrameworkCore<TDbContext>(this IMuleRegistrationBuilder builder)
        where TDbContext : DbContext
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        return builder.AddServices(services =>
        {
            services.AddMuleDbContextOptions<TDbContext>();
            services.TryAddScoped<IMuleDbContextFactory<TDbContext>, MuleDbContextFactory<TDbContext>>();
            services.TryAddScoped<IMuleStorage, EntityFrameworkMuleStorage<TDbContext>>();
            services.TryAddScoped<IMuleDiagnostics, EntityFrameworkMuleDiagnostics<TDbContext>>();
        });
    }

    public static IServiceCollection UseEntityFrameworkMule(
        this IServiceCollection services,
        Action<DbContextOptionsBuilder> configure)
    {
        if (services == null)
            throw new ArgumentNullException(nameof(services));

        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        services.AddDbContext<MuleDbContext>(configure);
        services.AddMuleDbContextOptions<MuleDbContext>();
        services.TryAddScoped<IMuleDbContextFactory<MuleDbContext>, MuleDbContextFactory<MuleDbContext>>();
        services.TryAddScoped<IMuleStorage, EntityFrameworkMuleStorage>();
        services.TryAddScoped<IMuleDiagnostics, EntityFrameworkMuleDiagnostics>();

        return services;
    }

    private static IServiceCollection AddMuleDbContextOptions<TDbContext>(this IServiceCollection services)
        where TDbContext : DbContext
    {
        var serviceType = typeof(DbContextOptions<TDbContext>);
        var descriptor = services.LastOrDefault(x => x.ServiceType == serviceType);

        if (descriptor == null)
            throw new InvalidOperationException($"DbContext '{typeof(TDbContext).Name}' must be registered before enabling Mule EF storage.");

        services.Remove(descriptor);
        services.Add(new ServiceDescriptor(serviceType, provider =>
        {
            var options = ResolveOptions<TDbContext>(descriptor, provider);
            return new DbContextOptionsBuilder<TDbContext>(options)
                .UseMuleModel()
                .Options;
        }, descriptor.Lifetime));

        return services;
    }

    private static DbContextOptions<TDbContext> ResolveOptions<TDbContext>(ServiceDescriptor descriptor, IServiceProvider provider)
        where TDbContext : DbContext
    {
        if (descriptor.ImplementationInstance is DbContextOptions<TDbContext> instance)
            return instance;

        if (descriptor.ImplementationFactory != null)
            return (DbContextOptions<TDbContext>)descriptor.ImplementationFactory(provider);

        if (descriptor.ImplementationType != null)
            return (DbContextOptions<TDbContext>)ActivatorUtilities.CreateInstance(provider, descriptor.ImplementationType);

        throw new InvalidOperationException($"Unable to resolve DbContextOptions for '{typeof(TDbContext).Name}'.");
    }
}
