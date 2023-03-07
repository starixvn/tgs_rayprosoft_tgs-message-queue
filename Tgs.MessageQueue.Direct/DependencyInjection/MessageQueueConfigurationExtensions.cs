using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.Direct;
using Tgs.MessageQueue.Direct.Configuration;

namespace Microsoft.Extensions.DependencyInjection;

public static class MessageQueueConfigurationExtensions
{
	public static MessageQueueConfiguration AddInProcessGlobPatternExchange(
		this MessageQueueConfiguration configuration,
		string glob)
		=> configuration.AddExchange(
			new DirectMessageExchange(glob, DirectMessageQueueConfiguration.SubscribeRegistrations));
}
