using System;
using Tgs.MessageQueue;
using Tgs.MessageQueue.RabbitMQ;
using Tgs.MessageQueue.RabbitMQ.Configuration;

namespace Microsoft.Extensions.DependencyInjection
{
	public static class ServiceCollectitonExtensions
	{
		public static IServiceCollection AddRabbitMessageQueue(
			this IServiceCollection services,
			Action<RabbitMessageQueueConfiguration> configure)
		{
			var configuration = new RabbitMessageQueueConfiguration(services);

			configure(configuration);

			return services
				.AddSingleton<RabbitMQConnectionManager>()
				.AddSingleton<IMessageReceiver<RabbitSubscriptionSettings>, RabbitMQConnectionManager>()
				.AddHostedService<MessageQueueBackground>()
				.AddOptions<RabbitMQOptions>()
				.Services;
		}
	}
}
