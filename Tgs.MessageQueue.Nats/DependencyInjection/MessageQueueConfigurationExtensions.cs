using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.Nats;

namespace Microsoft.Extensions.DependencyInjection;

public static class MessageQueueConfigurationExtensions
{
	public static MessageQueueConfiguration AddNatsGlobPatternExchange(
		this MessageQueueConfiguration configuration,
		string glob)
		=> configuration.AddGlobPatternExchange<INatsMessageQueueService>(glob);
}
