namespace Mule;

public interface IMuleAction<TPayload>
{
    ValueTask ExecuteAsync(MuleActionContext<TPayload> context, CancellationToken cancellationToken);
}
