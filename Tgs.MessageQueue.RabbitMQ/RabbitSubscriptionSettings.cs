using RabbitMQ.Client.Events;

namespace Tgs.MessageQueue.RabbitMQ
{
	record RabbitSubscriptionSettings
	{
		public string Subject { get; init; } = default!;

		public bool AutoAck { get; init; } = true;

		public AsyncEventHandler<BasicDeliverEventArgs> EventHandler { get; set; } = default!;
	}
}
