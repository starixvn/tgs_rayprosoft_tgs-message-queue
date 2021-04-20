using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration
{
	record ReplyRegistration<THandler>(string Subject) : ISubscribeRegistration where THandler : IMessageHandler
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
						var handler = ActivatorUtilities.CreateInstance<THandler>(scope.ServiceProvider);
						var replySender = scope.ServiceProvider.GetRequiredService<IMessageSender>();

						await handler
							.HandleAsync(args.Message.Data, cancellationToken)
							.ConfigureAwait(false);

						await replySender
							.PublishAsync(args.Message.Reply, Array.Empty<byte>())
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
