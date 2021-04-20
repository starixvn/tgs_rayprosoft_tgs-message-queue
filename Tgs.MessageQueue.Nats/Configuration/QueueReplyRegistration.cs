using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration
{
	record QueueReplyRegistration<THandler>(string Subject, string Queue) : ISubscribeRegistration where THandler : IMessageHandler
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
						var replySender = scope.ServiceProvider.GetRequiredService<IMessageSender>();

						await handler.HandleAsync(args.Message.Data, cancellationToken).ConfigureAwait(false);

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
