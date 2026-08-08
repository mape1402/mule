namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Mule;
using Mule.Configuration;
using Mule.Dispatching;

public static class MuleServiceCollectionExtensions
{
    public static IServiceCollection AddMule(
        this IServiceCollection services,
        Action<IMuleRegistrationBuilder> configure = null)
    {
        if (services == null)
            throw new ArgumentNullException(nameof(services));

        services.AddOptions<MuleSettings>();
        services.TryAddSingleton<IMuleSerializer, JsonMuleSerializer>();
        services.TryAddSingleton<MuleActionRegistry>();
        services.TryAddSingleton<IMuleActionRegistry>(provider => provider.GetRequiredService<MuleActionRegistry>());
        services.TryAddSingleton<IMuleDispatchQueue, ChannelMuleDispatchQueue>();
        services.TryAddSingleton<IMuleCommitNotifier, AmbientTransactionMuleCommitNotifier>();
        services.TryAddScoped<IMuleClient, MuleClient>();
        services.AddSingleton<IHostedService, MuleDispatcherHostedService>();

        if (configure != null)
        {
            using var provider = services.BuildServiceProvider();
            var registry = provider.GetRequiredService<MuleActionRegistry>();
            configure(new MuleRegistrationBuilder(registry));
            services.Replace(ServiceDescriptor.Singleton(registry));
            services.Replace(ServiceDescriptor.Singleton<IMuleActionRegistry>(registry));
        }

        return services;
    }
}
