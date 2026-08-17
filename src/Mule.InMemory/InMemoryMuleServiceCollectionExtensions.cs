namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.DependencyInjection.Extensions;
using Mule;
using Mule.Configuration;
using Mule.Diagnostics;
using Mule.InMemory;

public static class InMemoryMuleServiceCollectionExtensions
{
    public static IMuleRegistrationBuilder UseInMemory(this IMuleRegistrationBuilder builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        return builder.AddServices(services => services.UseInMemoryMule());
    }

    public static IServiceCollection UseInMemoryMule(this IServiceCollection services)
    {
        if (services == null)
            throw new ArgumentNullException(nameof(services));

        services.TryAddSingleton<InMemoryMuleStore>();
        services.TryAddSingleton<IInMemoryMule, InMemoryMule>();
        services.TryAddScoped<InMemoryMuleStorage>();
        services.TryAddScoped<IMuleDurableStorage>(provider => provider.GetRequiredService<InMemoryMuleStorage>());
        services.TryAddScoped<IMuleStorage>(provider => provider.GetRequiredService<InMemoryMuleStorage>());
        services.TryAddSingleton<IMuleDiagnostics, InMemoryMuleDiagnostics>();

        return services;
    }
}
