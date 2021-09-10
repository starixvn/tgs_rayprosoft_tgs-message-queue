using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.RabbitMQ.Configuration
{
	internal class SubscribeRegistration<THandler> : ISubscribeRegistration where THandler : IMessageHandler
	{
		private readonly bool _autoAck;

		public string Subject { get; }

		public SubscribeRegistration(string subject, bool autoAck)
		{
			if (string.IsNullOrEmpty(subject))
				throw new ArgumentException($"'{nameof(subject)}' is not Null or Empty.", nameof(subject));

			Subject = subject;
			_autoAck = autoAck;
		}

		public ValueTask<IDisposable> SubscribeAsync(
			IMessageReceiver<RabbitSubscriptionSettings> messageReceiver,
			IServiceProvider serviceProvider,
			ILogger logger,
			CancellationToken cancellationToken)
			=> messageReceiver.SubscribeAsync(new RabbitSubscriptionSettings
			{
				Subject = Subject,
				EventHandler = (sender, args) =>
				{
					return HandleMessageAsync(new MessageDataInfo
					{
						Args = args,
						ServiceProvider = serviceProvider,
						Logger = logger,
						CancellationToken = cancellationToken
					});
				}
			});

		private async Task HandleMessageAsync(MessageDataInfo dataInfo)
		{
			try
			{
				using var scope = dataInfo.ServiceProvider.CreateScope();
				var handler = ActivatorUtilities.CreateInstance<THandler>(scope.ServiceProvider);
				var connectionManager = scope.ServiceProvider.GetRequiredService<RabbitMQConnectionManager>();

				await handler
					.HandleAsync(dataInfo.Args.Body.ToArray(), dataInfo.CancellationToken)
					.ConfigureAwait(false);

				if (!_autoAck)
					connectionManager.MessageQueueChannel.BasicAck(dataInfo.Args.DeliveryTag, false);
			}
			catch (Exception ex)
			{
				dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");
			}
		}
	}
}
