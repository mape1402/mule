namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore.Metadata.Builders;

public sealed class MuleEntityFrameworkOptions
{
    private readonly List<Action<EntityTypeBuilder<DurableAction>>> _actionConfigurations = new();
    private readonly List<Action<EntityTypeBuilder<MuleReplay>>> _replayConfigurations = new();

    internal IReadOnlyCollection<Action<EntityTypeBuilder<DurableAction>>> ActionConfigurations => _actionConfigurations;

    internal IReadOnlyCollection<Action<EntityTypeBuilder<MuleReplay>>> ReplayConfigurations => _replayConfigurations;

    public MuleEntityFrameworkOptions ConfigureActions(Action<EntityTypeBuilder<DurableAction>> configure)
    {
        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        _actionConfigurations.Add(configure);
        return this;
    }

    public MuleEntityFrameworkOptions ConfigureReplays(Action<EntityTypeBuilder<MuleReplay>> configure)
    {
        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        _replayConfigurations.Add(configure);
        return this;
    }
}
