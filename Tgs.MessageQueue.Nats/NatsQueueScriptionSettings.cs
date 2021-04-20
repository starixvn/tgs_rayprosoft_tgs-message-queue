using System;
using NATS.Client;

namespace Tgs.MessageQueue.Nats
{
	internal struct NatsQueueScriptionSettings
	{
		public string Subject { get; set; }

		public string Queue { get; set; }

		public EventHandler<MsgHandlerEventArgs> EventHandler { get; set; }
	}
}
