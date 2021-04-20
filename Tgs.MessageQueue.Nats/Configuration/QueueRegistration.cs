using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration
{
	record QueueRegistration<THandler>(string Subject, string Queue) : ISubscribeRegistration where THandler : IMessageHandler
	{
		public ValueTask<IDisposable> SubscribeAsync(
			IMessageReceiver<NatsSubscriptionSettings> messageReceiver,
			IMessageReceiver<NatsQueueScriptionSettings> queueReceiver,
			IServiceProvider serviceProvider,
			ILogger logger,
			CancellationToken cancellationToken)
			=> queueReceiver.SubscribeAsync(new NatsQueueScriptionSettings
			{
				Subject = Subject,
				Queue = Queue,
				EventHandler = async (sender, args) =>
				 {
					 try
					 {
						 using var scope = serviceProvider.CreateScope();
						 var handler = ActivatorUtilities.CreateInstance<THandler>(scope.ServiceProvider);

						 await handler.HandleAsync(args.Message.Data, cancellationToken).ConfigureAwait(false);
					 }
					 catch (Exception ex)
					 {
						 logger.LogError(ex, $"Handle {Subject} occur error.");
					 }
				 }
			});
	}
}
