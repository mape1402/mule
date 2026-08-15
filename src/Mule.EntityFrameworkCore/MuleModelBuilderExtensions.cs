namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

public static class MuleModelBuilderExtensions
{
    public static ModelBuilder UseMuleModel(this ModelBuilder modelBuilder)
    {
        if (modelBuilder == null)
            throw new ArgumentNullException(nameof(modelBuilder));

        var actionKeyConverter = new ValueConverter<ActionKey, string>(
            key => key.Value,
            value => ActionKey.From(value));

        modelBuilder.Entity<DurableAction>(entity =>
        {
            entity.ToTable("MuleActions");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Key)
                .HasConversion(actionKeyConverter)
                .HasMaxLength(256)
                .IsRequired();
            entity.Property(x => x.Lane)
                .HasMaxLength(128)
                .IsRequired();
            entity.Property(x => x.Payload).IsRequired();
            entity.Property(x => x.PayloadType).HasMaxLength(1024).IsRequired();
            entity.Property(x => x.Metadata);
            entity.Property(x => x.CorrelationId).HasMaxLength(256);
            entity.Property(x => x.DeduplicationKey).HasMaxLength(512);
            entity.Property(x => x.Status).IsRequired();
            entity.Property(x => x.LastError);
            entity.Property(x => x.StartedOnUtc);
            entity.Property(x => x.TerminalOnUtc);
            entity.HasIndex(x => new { x.Lane, x.Status, x.NextAttemptOnUtc, x.CreatedOnUtc });
            entity.HasIndex(x => x.LockedOnUtc);
            entity.HasIndex(x => new { x.Key, x.DeduplicationKey })
                .IsUnique()
                .HasFilter("[DeduplicationKey] IS NOT NULL");
        });

        return modelBuilder;
    }
}
