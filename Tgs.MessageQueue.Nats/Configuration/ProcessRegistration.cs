using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration;

internal class ProcessRegistration<TProcessor> : ISubscribeRegistration
	where TProcessor : IMessageProcessor

{
	private readonly SessionRegistration<InternalProcessorSession<TProcessor>> _sessionRegistration;

	public string Subject { get; }

	public ProcessRegistration(string subject)
	{
		if (string.IsNullOrEmpty(subject))
			throw new ArgumentException($"'{nameof(subject)}' is not Null or Empty.", nameof(subject));
		Subject = subject;

		_sessionRegistration = new SessionRegistration<InternalProcessorSession<TProcessor>>(subject);
	}

	public ValueTask<IDisposable> SubscribeAsync(
		IMessageReceiver<NatsSubscriptionSettings> messageReceiver,
		IMessageReceiver<NatsQueueScriptionSettings> queueReceiver,
		IServiceProvider serviceProvider,
		ILogger logger,
		CancellationToken cancellationToken)
		=> _sessionRegistration.SubscribeAsync(
			messageReceiver,
			queueReceiver,
			serviceProvider,
			logger,
			cancellationToken);
}
