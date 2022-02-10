namespace Tgs.MessageQueue;

public interface IMessageHandler
{
	ValueTask HandleAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
}
