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
        var options = new MuleEntityFrameworkOptions();
        configure?.Invoke(options);

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
            entity.Property(x => x.CancelledOnUtc);
            entity.Property(x => x.TerminalOnUtc);
            entity.HasIndex(x => new { x.Lane, x.Status, x.NextAttemptOnUtc, x.CreatedOnUtc });
            entity.HasIndex(x => new { x.Status, x.LockedOnUtc });
            entity.HasIndex(x => new { x.Status, x.CompletedOnUtc });
            entity.HasIndex(x => new { x.Key, x.DeduplicationKey });

            foreach (var actionConfiguration in options.ActionConfigurations)
                actionConfiguration(entity);
        });

        modelBuilder.Entity<MuleReplay>(entity =>
        {
            entity.HasKey(x => x.RequestId);
            entity.Property(x => x.RequestId)
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
            entity.Property(x => x.SourceActionId).HasConversion(ulidConverter).HasMaxLength(26).IsRequired();
            entity.Property(x => x.ReplayRootActionId).HasConversion(ulidConverter).HasMaxLength(26).IsRequired();
            entity.Property(x => x.ReplayActionId).HasConversion(ulidConverter).HasMaxLength(26).IsRequired();
            entity.Property(x => x.BranchId).HasConversion(ulidConverter).HasMaxLength(26).IsRequired();
            entity.Property(x => x.LockToken).HasConversion(ulidConverter).HasMaxLength(26).IsRequired();
            entity.Property(x => x.ActionKey).HasConversion(actionKeyConverter).HasMaxLength(256).IsRequired();
            entity.Property(x => x.Lane).HasMaxLength(128).IsRequired();
            entity.Property(x => x.CorrelationId).HasMaxLength(128);
            entity.Property(x => x.OriginalDeduplicationKey).HasMaxLength(256);
            entity.Property(x => x.RequestedBy).HasMaxLength(256);
            entity.Property(x => x.Reason);
            entity.Property(x => x.Metadata);
            entity.Property(x => x.LastError);
            entity.Property(x => x.CancelledOnUtc);
            entity.HasIndex(x => x.ReplayActionId).IsUnique();
            entity.HasIndex(x => new { x.SourceActionId, x.BranchId }).IsUnique();
            entity.HasIndex(x => new { x.Status, x.ScheduleOnUtc, x.NextAttemptOnUtc });
            entity.HasIndex(x => new { x.Status, x.LockedOnUtc });

            foreach (var replayConfiguration in options.ReplayConfigurations)
                replayConfiguration(entity);
        });

        return modelBuilder;
    }
}
