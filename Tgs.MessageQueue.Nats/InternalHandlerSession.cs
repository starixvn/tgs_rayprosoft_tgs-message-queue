using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Nats.Configuration;

namespace Tgs.MessageQueue.Nats;

internal class InternalHandlerSession<THandler> : IMessageSession
	where THandler : IMessageHandler
{
	private readonly THandler _handler;

	public InternalHandlerSession(IServiceProvider serviceProvider)
	{
		_handler = ActivatorUtilities.CreateInstance<THandler>(serviceProvider);
	}

	public async ValueTask HandleAsync(Question question, CancellationToken cancellationToken = default)
	{
		using var activity = NatsMessageQueueConfiguration._NatsActivitySource.StartActivity("InternalHandlerSession");

		_ = (activity?.AddTag("mq", "NATS")
			.AddTag("handler", typeof(THandler).Name));

		await _handler
			.HandleAsync(question.Data, cancellationToken)
			.ConfigureAwait(false);

		await question
			.CompleteAsync(Array.Empty<byte>(), cancellationToken)
			.ConfigureAwait(false);
	}
}
