using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration;

internal interface ISubscribeRegistration
{
	ValueTask<IDisposable> SubscribeAsync(
		IMessageReceiver<NatsSubscriptionSettings> messageReceiver,
		IMessageReceiver<NatsQueueScriptionSettings> queueReceiver,
		IServiceProvider serviceProvider,
		ILogger logger,
		CancellationToken cancellationToken);
}
