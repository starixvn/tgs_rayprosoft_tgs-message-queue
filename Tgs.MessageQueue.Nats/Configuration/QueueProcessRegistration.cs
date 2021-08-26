using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration
{
	internal class QueueProcessRegistration<TProcessor> : ISubscribeRegistration
		where TProcessor : IMessageProcessor
	{
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
		}

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
				EventHandler = (sender, args) =>
				{
					_ = HandleMessageAsync(new MessageDataInfo
					{
						Args = args,
						ServiceProvider = serviceProvider,
						Logger = logger,
						CancellationToken = cancellationToken
					});
				}
			});

		private async ValueTask HandleMessageAsync(MessageDataInfo dataInfo)
		{
			try
			{
				using var scope = dataInfo.ServiceProvider.CreateScope();
				var handler = ActivatorUtilities.CreateInstance<TProcessor>(scope.ServiceProvider);
				var replySender = scope.ServiceProvider.GetRequiredService<IMessageSender>();

				await ProcessMessageAsync(dataInfo, handler, replySender).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");
			}
		}

		private async Task ProcessMessageAsync(
			MessageDataInfo dataInfo,
			TProcessor handler,
			IMessageSender replySender)
		{
			try
			{
				var replyData = await handler
					.HandleAsync(dataInfo.Args.Message.Data, dataInfo.CancellationToken)
					.ConfigureAwait(false);

				await replySender
					.PublishAsync(dataInfo.Args.Message.Reply, replyData)
					.ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");

				if (handler is IMessageExceptionProcessor exceptionProcessor)
					await exceptionProcessor
						.HandleExceptionAsync(dataInfo.Args.Message.Data, ex, dataInfo.CancellationToken)
						.ConfigureAwait(false);
				else
					await replySender
						.PublishFailAsync(dataInfo.Args.Message.Reply, Array.Empty<byte>())
						.ConfigureAwait(false);
			}
		}
	}
}
