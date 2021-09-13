using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tgs.MessageQueue.RabbitMQ.Configuration;

namespace Tgs.MessageQueue.RabbitMQ
{
	internal class MessageQueueBackground : BackgroundService
	{
		private readonly ILogger<MessageQueueBackground> _logger;
		private readonly IMessageReceiver<RabbitSubscriptionSettings> _messageReceiver;
		private readonly IServiceProvider _serviceProvider;
		private readonly IEnumerable<ISubscribeRegistration> _subscribes;

		public MessageQueueBackground(
			IMessageReceiver<RabbitSubscriptionSettings> messageReceiver,
			IServiceProvider serviceProvider,
			IEnumerable<ISubscribeRegistration> subscribes,
			ILogger<MessageQueueBackground> logger)
		{
			_messageReceiver = messageReceiver ?? throw new ArgumentNullException(nameof(messageReceiver));
			_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
			_subscribes = subscribes ?? throw new ArgumentNullException(nameof(subscribes));
			_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		}

		protected override async Task ExecuteAsync(CancellationToken stoppingToken)
		{
			var subscriptions = new List<IDisposable>();

			foreach (var registration in _subscribes)
				subscriptions.Add(
					await registration.SubscribeAsync(
						_messageReceiver,
						_serviceProvider,
						_logger,
						stoppingToken).ConfigureAwait(false));

			stoppingToken.Register(() =>
			{
				foreach (var sub in subscriptions)
					sub.Dispose();
			});
		}
	}
}
