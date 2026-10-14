namespace Mule.EntityFrameworkCore.Internal;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

internal sealed class MuleModelCustomizer : ModelCustomizer
{
    public MuleModelCustomizer(ModelCustomizerDependencies dependencies)
        : base(dependencies)
    {
    }

    public override void Customize(ModelBuilder modelBuilder, DbContext context)
    {
        base.Customize(modelBuilder, context);
        var extension = context.GetService<IDbContextOptions>()
            .FindExtension<MuleDbContextOptionsExtension>();

        modelBuilder.UseMuleModel(extension?.ConfigureModel);
    }
}
