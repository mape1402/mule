namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore.Metadata.Builders;

public sealed class MuleEntityFrameworkOptions
{
    private readonly List<Action<EntityTypeBuilder<DurableAction>>> _actionConfigurations = new();

    internal IReadOnlyCollection<Action<EntityTypeBuilder<DurableAction>>> ActionConfigurations => _actionConfigurations;

    public MuleEntityFrameworkOptions ConfigureActions(Action<EntityTypeBuilder<DurableAction>> configure)
    {
        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        _actionConfigurations.Add(configure);
        return this;
    }
}
