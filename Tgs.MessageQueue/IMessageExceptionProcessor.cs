namespace Tgs.MessageQueue;

public interface IMessageExceptionProcessor
{
	ValueTask<ReadOnlyMemory<byte>> HandleExceptionAsync(ReadOnlyMemory<byte> data, Exception exception, CancellationToken cancellationToken = default);
}
