namespace Tgs.MessageQueue.InProcess;

internal class MessageHandlerExecutor<THandler> : IMessageHandlerExecutor
	where THandler : class, IMessageHandler
{
	private readonly THandler _handler;

	public MessageHandlerExecutor(THandler handler)
	{
		_handler = handler ?? throw new ArgumentNullException(nameof(handler));
	}

	public async ValueTask HandleAsync(InProcessMessage message, CancellationToken cancellationToken = default)
	{
		await _handler.HandleAsync(
			message.Message,
			cancellationToken).ConfigureAwait(false);

		message.CompletionSource.SetResult(Array.Empty<byte>());
	}
}
