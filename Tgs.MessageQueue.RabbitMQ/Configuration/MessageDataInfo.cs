using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;

namespace Tgs.MessageQueue.RabbitMQ.Configuration
{
	internal class MessageDataInfo
	{
		public BasicDeliverEventArgs Args { get; init; } = default!;

		public CancellationToken CancellationToken { get; init; }

		public ILogger Logger { get; init; } = default!;

		public IServiceProvider ServiceProvider { get; init; } = default!;
	}
}
