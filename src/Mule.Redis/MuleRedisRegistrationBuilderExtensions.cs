namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.DependencyInjection.Extensions;
using Mule;
using Mule.Configuration;
using Mule.Diagnostics;
using Mule.Redis;

public static class MuleRedisRegistrationBuilderExtensions
{
    public static IMuleRegistrationBuilder UseRedis(
        this IMuleRegistrationBuilder builder,
        Action<RedisMuleOptions> configure = null)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        return builder.AddServices(services =>
        {
            if (services.Any(x => x.ServiceType == typeof(IMuleDurableStorage)))
                throw new InvalidOperationException("Mule already has a durable storage provider. Configure exactly one authoritative provider.");

            if (configure != null)
                services.Configure(configure);
            services.AddOptions<RedisMuleOptions>();
            services.TryAddSingleton<RedisMuleConnection>();
            services.TryAddScoped<RedisMuleStorage>();
            services.TryAddScoped<IMuleDurableStorage>(provider => provider.GetRequiredService<RedisMuleStorage>());
            services.TryAddScoped<IMuleStorage>(provider => provider.GetRequiredService<RedisMuleStorage>());
            services.TryAddScoped<IMuleReplayStorage>(provider => provider.GetRequiredService<RedisMuleStorage>());
            services.TryAddScoped<IMuleCancellationStorage>(provider => provider.GetRequiredService<RedisMuleStorage>());
            services.TryAddScoped<IMuleTerminalCancellationStorage>(provider => provider.GetRequiredService<RedisMuleStorage>());
            services.TryAddScoped<IMuleBatchTerminalStorage>(provider => provider.GetRequiredService<RedisMuleStorage>());
            services.TryAddScoped<IMuleDiagnostics, RedisMuleDiagnostics>();
        });
    }
}
