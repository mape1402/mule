namespace Mule.Testing;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Mule.Configuration;
using Mule.InMemory;

public static class MuleTestingRegistrationBuilderExtensions
{
    public static IMuleRegistrationBuilder UseTesting(this IMuleRegistrationBuilder builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        return builder.AddServices(services =>
        {
            services.UseInMemoryMule();
            services.TryAddSingleton<IMuleTestHarness, MuleTestHarness>();
        });
    }
}
