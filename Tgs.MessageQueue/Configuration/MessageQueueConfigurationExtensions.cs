using Tgs.MessageQueue.Exchanges;

namespace Tgs.MessageQueue.Configuration
{
	public static class MessageQueueConfigurationExtensions
	{
		public static MessageQueueConfiguration AddGlobPatternExchange<TMessageSender>(
			this MessageQueueConfiguration configuration,
			string glob)
			where TMessageSender : class, IMessageSender
			=> configuration.AddExchange(new GlobMessageExchange<TMessageSender>(glob));
	}
}
