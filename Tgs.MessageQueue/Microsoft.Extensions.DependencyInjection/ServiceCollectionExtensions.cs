using Tgs.MessageQueue;
using Tgs.MessageQueue.Configuration;

namespace Microsoft.Extensions.DependencyInjection;

public static class ServiceCollectionExtensions
{
	public static MessageQueueConfiguration AddMessageQueue(this IServiceCollection services)
	{
		var configuration = new MessageQueueConfiguration(services);

		_ = services.AddTransient<IMessageSender>(
			sp => new MultiplexerMessageSender(sp, configuration.GetRegisterExchanges()));

		return configuration;
	}
}
