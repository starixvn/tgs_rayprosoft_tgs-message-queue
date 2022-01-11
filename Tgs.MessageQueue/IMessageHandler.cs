namespace Tgs.MessageQueue;

public interface IMessageHandler
{
	ValueTask HandleAsync(byte[] data, CancellationToken cancellationToken = default);
}
