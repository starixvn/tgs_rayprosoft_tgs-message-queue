using Tgs.MessageQueue.Compatible;
using Valhalla.MessageQueue.Configuration;

namespace Microsoft.Extensions.DependencyInjection;

extern alias Tgs;

public static class ServiceCollectionExtensions
{
	public static MessageQueueConfiguration AddPhoenixToolkitsMessageQueue(this IServiceCollection services)
		=> services
			.AddTransient<Tgs::Tgs.MessageQueue.IMessageSender, TgsMessageSenderWrapper>()
			.AddMessageQueue();
}
