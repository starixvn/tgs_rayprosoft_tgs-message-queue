using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration
{
	record ProcessRegistration<TProcessor>(string Subject) : ISubscribeRegistration where TProcessor : IMessageProcessor
	{
		public ValueTask<IDisposable> SubscribeAsync(
			IMessageReceiver<NatsSubscriptionSettings> messageReceiver,
			IMessageReceiver<NatsQueueScriptionSettings> queueReceiver,
			IServiceProvider serviceProvider,
			ILogger logger,
			CancellationToken cancellationToken) 
			=> messageReceiver.SubscribeAsync(new NatsSubscriptionSettings
			{
				Subject = Subject,
				EventHandler = async (sender, args) =>
				{
					try
					{
						using var scope = serviceProvider.CreateScope();
						var handler = ActivatorUtilities.CreateInstance<TProcessor>(scope.ServiceProvider);
						var replySender = scope.ServiceProvider.GetRequiredService<IMessageSender>();

						var replyData = await handler
							.HandleAsync(args.Message.Data, cancellationToken)
							.ConfigureAwait(false);

						await replySender
							.PublishAsync(args.Message.Reply, replyData)
							.ConfigureAwait(false);
					}
					catch (Exception ex)
					{
						logger.LogError(ex, $"Handle {Subject} occur error.");
					}
				}
			});
	}
}
