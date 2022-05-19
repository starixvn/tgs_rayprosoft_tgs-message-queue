using RabbitMQ.Client;

namespace Tgs.MessageQueue.RabbitMQ;

internal class NoopMessageQueueServiceFactory : IMessageQueueServiceFactory
{
	public IMessageQueueService CreateMessageQueueService(IServiceProvider serviceProvider, string exchangeName, IModel channel)
		=> new NoopMessageQueueService();
}
