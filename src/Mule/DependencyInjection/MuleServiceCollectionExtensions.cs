namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Mule;
using Mule.Configuration;
using Mule.Diagnostics;
using Mule.Dispatching;
using Mule.Replay;

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
        services.TryAddSingleton<MuleRuntimeMetrics>();
        services.TryAddSingleton<MuleActionRegistry>();
        services.TryAddSingleton<IMuleActionRegistry>(provider => provider.GetRequiredService<MuleActionRegistry>());
        services.TryAddSingleton<IMuleDispatchQueue, ChannelMuleDispatchQueue>();
        services.TryAddSingleton<MuleActionExecutor>();
        services.TryAddSingleton<IMuleActionExecutor>(provider => provider.GetRequiredService<MuleActionExecutor>());
        services.TryAddSingleton<MuleSchedulerSignal>();
        services.TryAddSingleton<IMuleCommitNotifier, AmbientTransactionMuleCommitNotifier>();
        services.TryAddSingleton<IMuleReplayCommitNotifier, AmbientTransactionMuleReplayCommitNotifier>();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddScoped<IMuleClient, MuleClient>();
        services.TryAddScoped<IMuleReplayService, MuleReplayService>();
        services.AddSingleton<IHostedService, MuleDispatcherHostedService>();
        services.AddSingleton<IHostedService, MuleReplayHostedService>();

        var registry = new MuleActionRegistry();

        if (configure != null)
            configure(new MuleRegistrationBuilder(services, registry));

        services.Replace(ServiceDescriptor.Singleton(registry));
        services.Replace(ServiceDescriptor.Singleton<IMuleActionRegistry>(registry));

        return services;
    }
}
