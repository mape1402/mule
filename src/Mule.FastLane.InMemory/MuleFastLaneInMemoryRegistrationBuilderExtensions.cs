namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.DependencyInjection.Extensions;
using Mule;
using Mule.Configuration;
using Mule.FastLane.InMemory;

public static class MuleFastLaneInMemoryRegistrationBuilderExtensions
{
    public static IMuleRegistrationBuilder UseFastLaneInMemory(
        this IMuleRegistrationBuilder builder,
        Action<FastLaneInMemoryOptions> configure = null)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        return builder.AddServices(services =>
        {
            if (configure != null)
                services.Configure(configure);

            services.AddOptions<FastLaneInMemoryOptions>();
            services.TryAddSingleton<InMemoryFastLaneBuffer>();
            services.Replace(ServiceDescriptor.Scoped<IMuleStorage, FastLaneInMemoryStorage>());
            services.AddHostedService<FastLaneInMemoryFlushService>();
        });
    }
}
