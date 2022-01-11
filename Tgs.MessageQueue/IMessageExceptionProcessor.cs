namespace Tgs.MessageQueue;

public interface IMessageExceptionProcessor
{
	ValueTask<byte[]> HandleExceptionAsync(byte[] data, Exception exception, CancellationToken cancellationToken = default);
}
