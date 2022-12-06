namespace Tgs.MessageQueue.InProcess;

internal interface IMessageHandlerExecutor
{
	ValueTask HandleAsync(InProcessMessage message, CancellationToken cancellationToken = default);
}
