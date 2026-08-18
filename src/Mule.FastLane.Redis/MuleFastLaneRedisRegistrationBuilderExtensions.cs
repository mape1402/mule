namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.DependencyInjection.Extensions;
using Mule;
using Mule.Configuration;
using Mule.FastLane.Redis;

public static class MuleFastLaneRedisRegistrationBuilderExtensions
{
    public static IMuleRegistrationBuilder UseFastLaneRedis(
        this IMuleRegistrationBuilder builder,
        Action<FastLaneRedisOptions> configure)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        return builder.AddServices(services =>
        {
            services.Configure(configure);
            services.AddOptions<FastLaneRedisOptions>();
            services.TryAddSingleton<RedisFastLaneConnection>();
            services.TryAddSingleton<RedisFastLaneBuffer>();
            services.Replace(ServiceDescriptor.Scoped<IMuleStorage, FastLaneRedisStorage>());
            services.AddHostedService<FastLaneRedisFlushService>();
        });
    }
}
