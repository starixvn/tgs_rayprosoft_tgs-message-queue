using System;
using NATS.Client;

namespace Tgs.MessageQueue.Nats
{
	internal record NatsQueueScriptionSettings
	{
		public string Subject { get; set; } = default!;

		public string Queue { get; set; } = default!;

		public EventHandler<MsgHandlerEventArgs> EventHandler { get; set; } = default!;
	}
}
