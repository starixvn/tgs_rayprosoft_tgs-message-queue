using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client;
using Tgs.MessageQueue;
using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.Nats;
using Tgs.MessageQueue.Nats.Configuration;

namespace Microsoft.Extensions.DependencyInjection;

public static class ServiceCollectionExtensions
{
	public static IServiceCollection AddFakeNatsMessageQueue(this IServiceCollection services)
	{
		foreach (var desc in services.Where(
			desc => desc.ServiceType == typeof(IHostedService)
				&& desc.ImplementationType == typeof(MessageQueueBackground)).ToArray())
		{
			_ = services.Remove(desc);
		}

		return services
			.AddSingleton<NoopMessageQueueService>()
			.AddTransient<IMessageSender>(sp => sp.GetRequiredService<NoopMessageQueueService>())
			.AddTransient<IMessageReceiver<NatsSubscriptionSettings>>(
				sp => sp.GetRequiredService<NoopMessageQueueService>())
			.AddTransient<IMessageReceiver<NatsQueueScriptionSettings>>(
				sp => sp.GetRequiredService<NoopMessageQueueService>());
	}

	public static IServiceCollection AddNatsMessageQueue(
				this IServiceCollection services,
		Action<NatsMessageQueueConfiguration> configure)
	{
		var configuration = new NatsMessageQueueConfiguration(services);

		configure(configuration);

		InitialNatsMessageQueueConfiguration(configuration);

		return services;
	}

	public static MessageQueueConfiguration AddNatsMessageQueue(
		this MessageQueueConfiguration configuration,
		Action<NatsMessageQueueConfiguration> configure)
	{
		var natsConfiguration = new NatsMessageQueueConfiguration(configuration);

		configure(natsConfiguration);

		InitialNatsMessageQueueConfiguration(natsConfiguration);

		return configuration;
	}

	private static void InitialNatsMessageQueueConfiguration(NatsMessageQueueConfiguration configuration)
	{
		_ = configuration.Services
			.AddSingleton<ConnectionFactory>()
			.AddSingleton(sp =>
			{
				var connectionFactory = sp.GetRequiredService<ConnectionFactory>();
				var queueOptions = sp.GetRequiredService<IOptions<NatsOptions>>().Value;
				var logger = sp.GetRequiredService<ILogger<NatsOptions>>();

				logger.LogDebug($"Nats connection url: {queueOptions.Url}");

				var options = ConnectionFactory.GetDefaultOptions();

				options.Url = queueOptions.Url;

				return ActivatorUtilities.CreateInstance<NatsMessageQueueService>(sp, connectionFactory.CreateConnection(options));
			})
			.AddTransient<IMessageReceiver<NatsSubscriptionSettings>>(
				sp => sp.GetRequiredService<NatsMessageQueueService>())
			.AddTransient<IMessageReceiver<NatsQueueScriptionSettings>>(
				sp => sp.GetRequiredService<NatsMessageQueueService>())
			.AddHostedService<MessageQueueBackground>()
			.AddOptions<NatsOptions>();
	}
}
