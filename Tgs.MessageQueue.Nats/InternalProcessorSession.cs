using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Nats.Configuration;

namespace Tgs.MessageQueue.Nats;

internal class InternalProcessorSession<TProcessor> : IMessageSession
	where TProcessor : IMessageProcessor
{
	private readonly TProcessor _processor;

	public InternalProcessorSession(IServiceProvider serviceProvider)
	{
		_processor = ActivatorUtilities.CreateInstance<TProcessor>(serviceProvider);
	}

	public async ValueTask HandleAsync(Question question, CancellationToken cancellationToken = default)
	{
		using var activity = NatsMessageQueueConfiguration._NatsActivitySource.StartActivity($"InternalHandlerSession");

		_ = (activity?.AddTag("mq", "NATS")
			.AddTag("handler", typeof(TProcessor).Name));

		var result = await _processor
			.HandleAsync(question.Data, cancellationToken)
			.ConfigureAwait(false);

		await question
			.CompleteAsync(Array.Empty<byte>(), cancellationToken)
			.ConfigureAwait(false);
	}
}
