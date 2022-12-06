namespace Tgs.MessageQueue.InProcess;

internal class MessageProcessorExecutor<TProcessor> : IMessageHandlerExecutor
	where TProcessor : class, IMessageProcessor
{
	private readonly TProcessor _processor;

	public MessageProcessorExecutor(TProcessor processor)
	{
		_processor = processor ?? throw new ArgumentNullException(nameof(processor));
	}

	public async ValueTask HandleAsync(InProcessMessage message, CancellationToken cancellationToken = default)
	{
		var response = await _processor.HandleAsync(
			message.Message,
			cancellationToken).ConfigureAwait(false);

		message.CompletionSource.SetResult(response);
	}
}
