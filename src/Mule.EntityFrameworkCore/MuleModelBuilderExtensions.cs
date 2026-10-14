namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

public static class MuleModelBuilderExtensions
{
    public static ModelBuilder UseMuleModel(
        this ModelBuilder modelBuilder,
        Action<MuleEntityFrameworkOptions> configure = null)
    {
        if (modelBuilder == null)
            throw new ArgumentNullException(nameof(modelBuilder));

        var actionKeyConverter = new ValueConverter<ActionKey, string>(
            key => key.Value,
            value => ActionKey.From(value));
        var ulidConverter = new ValueConverter<Ulid, string>(
            id => id.ToString(),
            value => Ulid.Parse(value));

        modelBuilder.Entity<DurableAction>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id)
                .HasConversion(ulidConverter)
                .HasMaxLength(26)
                .IsRequired()
                .ValueGeneratedNever();
            entity.Property(x => x.ConcurrencyToken)
                .HasConversion(ulidConverter)
                .HasMaxLength(26)
                .IsConcurrencyToken()
                .IsRequired()
                .ValueGeneratedNever();
            entity.Property(x => x.Key)
                .HasConversion(actionKeyConverter)
                .HasMaxLength(256)
                .IsRequired();
            entity.Property(x => x.Lane)
                .HasMaxLength(128)
                .IsRequired();
            entity.Property(x => x.Payload).IsRequired();
            entity.Property(x => x.PayloadType).HasMaxLength(512).IsRequired();
            entity.Property(x => x.Metadata);
            entity.Property(x => x.CorrelationId).HasMaxLength(128);
            entity.Property(x => x.DeduplicationKey).HasMaxLength(256);
            entity.Property(x => x.Status).IsRequired();
            entity.Property(x => x.LastError);
            entity.Property(x => x.StartedOnUtc);
            entity.Property(x => x.TerminalOnUtc);
            entity.HasIndex(x => new { x.Lane, x.Status, x.NextAttemptOnUtc, x.CreatedOnUtc });
            entity.HasIndex(x => new { x.Status, x.LockedOnUtc });
            entity.HasIndex(x => new { x.Status, x.CompletedOnUtc });
            entity.HasIndex(x => new { x.Key, x.DeduplicationKey });

            var options = new MuleEntityFrameworkOptions();
            configure?.Invoke(options);
            foreach (var actionConfiguration in options.ActionConfigurations)
                actionConfiguration(entity);
        });

        return modelBuilder;
    }
}
