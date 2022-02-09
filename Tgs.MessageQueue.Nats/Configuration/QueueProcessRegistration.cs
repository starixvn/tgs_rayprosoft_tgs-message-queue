using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration;

internal class QueueProcessRegistration<TProcessor> : ISubscribeRegistration
	where TProcessor : IMessageProcessor
{
	private readonly QueueSessionRegistration<InternalProcessorSession<TProcessor>> _queueSessionRegistration;

	public string Queue { get; }

	public string Subject { get; }

	public QueueProcessRegistration(string subject, string queue)
	{
		if (string.IsNullOrEmpty(subject))
			throw new ArgumentException($"'{nameof(subject)}' is not Null or Empty.", nameof(subject));
		if (string.IsNullOrEmpty(queue))
			throw new ArgumentException($"'{nameof(queue)}' is not Null or Empty.", nameof(queue));
		Subject = subject;
		Queue = queue;

		_queueSessionRegistration = new QueueSessionRegistration<InternalProcessorSession<TProcessor>>(subject, queue);
	}

	public ValueTask<IDisposable> SubscribeAsync(
		IMessageReceiver<NatsSubscriptionSettings> messageReceiver,
		IMessageReceiver<NatsQueueScriptionSettings> queueReceiver,
		IServiceProvider serviceProvider,
		ILogger logger,
		CancellationToken cancellationToken)
		=> _queueSessionRegistration.SubscribeAsync(
			messageReceiver,
			queueReceiver,
			serviceProvider,
			logger,
			cancellationToken);
}
