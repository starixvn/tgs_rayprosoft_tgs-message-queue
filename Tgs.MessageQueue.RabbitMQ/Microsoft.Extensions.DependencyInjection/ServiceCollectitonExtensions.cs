using System;
using Tgs.MessageQueue;
using Tgs.MessageQueue.Configuration;
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

			InitialRabbitMessageQueueConfiguration(configuration);

			return services;
		}

		public static MessageQueueConfiguration AddRabbitMessageQueue(
			this MessageQueueConfiguration configuration,
			Action<RabbitMessageQueueConfiguration> configure)
		{
			var rabbitConfiguration = new RabbitMessageQueueConfiguration(configuration);

			configure(rabbitConfiguration);

			InitialRabbitMessageQueueConfiguration(rabbitConfiguration);

			return configuration;
		}

		private static void InitialRabbitMessageQueueConfiguration(RabbitMessageQueueConfiguration configuration)
		{
			configuration.Services
				.AddSingleton<RabbitMQConnectionManager>()
				.AddSingleton<IMessageReceiver<RabbitSubscriptionSettings>, RabbitMQConnectionManager>()
				.AddHostedService<MessageQueueBackground>()
				.AddOptions<RabbitMQOptions>();
		}
	}
}
