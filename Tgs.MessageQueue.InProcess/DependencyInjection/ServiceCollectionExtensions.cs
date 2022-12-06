using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.InProcess;
using Tgs.MessageQueue.InProcess.Configuration;

namespace Microsoft.Extensions.DependencyInjection;

public static class ServiceCollectionExtensions
{
	public static MessageQueueConfiguration AddInProcessMessageQueue(
		this MessageQueueConfiguration configuration,
		Action<InProcessMessageQueueConfiguration> configure)
	{
		var inProcessConfiguration = new InProcessMessageQueueConfiguration(configuration);

		configure(inProcessConfiguration);

		configuration.Services
			.AddSingleton<InProcessMessageQueue>()
			.AddHostedService<MessageQueueBackground>();

		return configuration;
	}
}
