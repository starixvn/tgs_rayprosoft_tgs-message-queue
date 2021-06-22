using System;
using Microsoft.Extensions.Options;
using NATS.Client;
using Tgs.MessageQueue;
using Tgs.MessageQueue.Nats;
using Tgs.MessageQueue.Nats.Configuration;
using System.Linq;
using Microsoft.Extensions.Hosting;

namespace Microsoft.Extensions.DependencyInjection
{
	public static class ServiceCollectionExtensions
	{
		public static IServiceCollection AddNatsMessageQueue(
			this IServiceCollection services,
			Action<NatsMessageQueueConfiguration> configure)
		{
			var configuration = new NatsMessageQueueConfiguration(services);

			configure(configuration);

			return services
				.AddSingleton<ConnectionFactory>()
				.AddSingleton(sp =>
				{
					var connectionFactory = sp.GetRequiredService<ConnectionFactory>();
					var queueOptions = sp.GetRequiredService<IOptions<NatsOptions>>().Value;

					var options = ConnectionFactory.GetDefaultOptions();

					options.Url = queueOptions.Url;

					return ActivatorUtilities.CreateInstance<NatsMessageQueueService>(sp, connectionFactory.CreateConnection(options));
				})
				.AddTransient<IMessageSender>(sp => sp.GetRequiredService<NatsMessageQueueService>())
				.AddTransient<IMessageReceiver<NatsSubscriptionSettings>>(
					sp => sp.GetRequiredService<NatsMessageQueueService>())
				.AddTransient<IMessageReceiver<NatsQueueScriptionSettings>>(
					sp => sp.GetRequiredService<NatsMessageQueueService>())
				.AddHostedService<MessageQueueBackground>()
				.AddOptions<NatsOptions>()
				.Services;
		}

		public static IServiceCollection AddFakeNatsMessageQueue(this IServiceCollection services)
		{

			foreach (var desc in services.Where(
				desc => desc.ServiceType == typeof(IHostedService)
					&& desc.ImplementationType == typeof(MessageQueueBackground)).ToArray())
			{
				services.Remove(desc);
			}

			return services
				.AddSingleton<NoopMessageQueueService>()
				.AddTransient<IMessageSender>(sp => sp.GetRequiredService<NoopMessageQueueService>())
				.AddTransient<IMessageReceiver<NatsSubscriptionSettings>>(
					sp => sp.GetRequiredService<NoopMessageQueueService>())
				.AddTransient<IMessageReceiver<NatsQueueScriptionSettings>>(
					sp => sp.GetRequiredService<NoopMessageQueueService>());
		}
	}
}
