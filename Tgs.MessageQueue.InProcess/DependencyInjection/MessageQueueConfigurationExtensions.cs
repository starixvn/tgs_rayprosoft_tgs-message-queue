using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.InProcess;

namespace Microsoft.Extensions.DependencyInjection;

public static class MessageQueueConfigurationExtensions
{
	public static MessageQueueConfiguration AddInProcessGlobPatternExchange(
		this MessageQueueConfiguration configuration,
		string glob)
		=> configuration.AddGlobPatternExchange<InProcessMessageQueue>(glob);
}
