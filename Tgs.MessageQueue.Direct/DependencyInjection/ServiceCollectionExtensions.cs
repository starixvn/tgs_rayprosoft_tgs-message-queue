using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.Direct;
using Tgs.MessageQueue.Direct.Configuration;

namespace Microsoft.Extensions.DependencyInjection;

public static class ServiceCollectionExtensions
{
	public static MessageQueueConfiguration AddDirectMessageQueue(
		this MessageQueueConfiguration configuration,
		Action<DirectMessageQueueConfiguration> configure)
	{
		var inProcessConfiguration = new DirectMessageQueueConfiguration(configuration);

		configure(inProcessConfiguration);

		_ = configuration.Services
			.AddSingleton(typeof(DirectHandlerMessageSender<>))
			.AddSingleton(typeof(DirectProcessorMessageSender<>))
			.AddSingleton(typeof(DirectSessionMessageSender<>));

		return configuration;
	}
}
