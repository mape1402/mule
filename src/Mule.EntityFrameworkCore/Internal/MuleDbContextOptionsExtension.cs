namespace Mule.EntityFrameworkCore.Internal;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

internal sealed class MuleDbContextOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo _info;

    public MuleDbContextOptionsExtension(Action<MuleEntityFrameworkOptions> configureModel = null)
    {
        ConfigureModel = configureModel;
    }

    public Action<MuleEntityFrameworkOptions> ConfigureModel { get; }

    public DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
    {
        services.Replace(ServiceDescriptor.Singleton<IModelCustomizer, MuleModelCustomizer>());
    }

    public void Validate(IDbContextOptions options)
    {
    }

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        private readonly MuleDbContextOptionsExtension _extension;

        public ExtensionInfo(MuleDbContextOptionsExtension extension)
            : base(extension)
        {
            _extension = extension;
        }

        public override bool IsDatabaseProvider => false;

        public override string LogFragment => "using Mule ";

        public override int GetServiceProviderHashCode()
            => HashCode.Combine(
                typeof(MuleDbContextOptionsExtension),
                _extension.ConfigureModel?.GetHashCode() ?? 0);

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            => debugInfo["Mule"] = "1";

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;
    }
}
