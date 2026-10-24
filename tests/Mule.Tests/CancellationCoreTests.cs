namespace Mule.Tests;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Mule.Cancellation;
using NSubstitute;

public sealed class CancellationCoreTests
{
    [Fact]
    public async Task CancelAsync_Should_Reject_Empty_Action_Id()
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        var service = CreateService(services, DateTimeOffset.UtcNow);

        await Assert.ThrowsAsync<ArgumentException>(async () => await service.CancelAsync(Ulid.Empty));
    }

    [Fact]
    public async Task CancelAsync_Should_Return_NotSupported_When_Storage_Has_No_Capability()
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        var id = Ulid.NewUlid();

        var result = await CreateService(services, DateTimeOffset.UtcNow).CancelAsync(id);

        Assert.Equal(id, result.ActionId);
        Assert.Equal(MuleCancellationOutcome.NotSupported, result.Outcome);
        Assert.False(result.IsCancelled);
    }

    [Theory]
    [InlineData(MuleCancellationOutcome.Cancelled, true)]
    [InlineData(MuleCancellationOutcome.AlreadyCancelled, true)]
    [InlineData(MuleCancellationOutcome.TooLate, false)]
    [InlineData(MuleCancellationOutcome.NotFound, false)]
    public async Task CancelAsync_Should_Forward_Storage_Outcome(
        MuleCancellationOutcome outcome,
        bool isCancelled)
    {
        var id = Ulid.NewUlid();
        var now = DateTimeOffset.UtcNow;
        var storage = Substitute.For<IMuleCancellationStorage>();
        storage.CancelAsync(id, now, Arg.Any<CancellationToken>())
            .Returns(new MuleCancellationResult(id, outcome, isCancelled ? now : null));
        using var services = new ServiceCollection()
            .AddSingleton(storage)
            .BuildServiceProvider();

        var result = await CreateService(services, now).CancelAsync(id);

        Assert.Equal(outcome, result.Outcome);
        Assert.Equal(isCancelled, result.IsCancelled);
        await storage.Received(1).CancelAsync(id, now, Arg.Any<CancellationToken>());
    }

    [Fact]
    public void CancellationService_Should_Validate_Dependencies()
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        var logger = NullLogger<MuleCancellationService>.Instance;

        Assert.Throws<ArgumentNullException>(() => new MuleCancellationService(null, TimeProvider.System, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleCancellationService(services, null, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleCancellationService(services, TimeProvider.System, null));
    }

    [Fact]
    public void AddMule_Should_Register_Cancellation_Service()
    {
        var services = new ServiceCollection();

        services.AddMule();

        Assert.Contains(services, x => x.ServiceType == typeof(IMuleCancellationService));
    }

    private static MuleCancellationService CreateService(IServiceProvider services, DateTimeOffset now)
        => new(
            services,
            new FixedTimeProvider(now),
            NullLogger<MuleCancellationService>.Instance);

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(DateTimeOffset now)
            => _now = now;

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
