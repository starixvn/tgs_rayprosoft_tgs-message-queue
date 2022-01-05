using System;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Tgs.MessageQueue.RabbitMQ
{
	record RabbitSubscriptionSettings
	{
		public string Subject { get; init; } = default!;

		public bool AutoAck { get; init; } = true;

		public int ConsumerDispatchConcurrency { get; init; } = 1;

		public Func<IModel, BasicDeliverEventArgs, Task> EventHandler { get; set; } = default!;
	}
}
