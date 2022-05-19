using RabbitMQ.Client;

namespace Tgs.MessageQueue.RabbitMQ;

internal interface IMessageQueueServiceFactory
{
	IMessageQueueService CreateMessageQueueService(IServiceProvider serviceProvider, string exchangeName, IModel channel);
}
