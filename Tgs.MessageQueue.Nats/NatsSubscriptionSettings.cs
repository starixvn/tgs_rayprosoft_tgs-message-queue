using System;
using NATS.Client;

namespace Tgs.MessageQueue.Nats
{
	internal struct NatsSubscriptionSettings
	{
		public string Subject { get; set; }

		public EventHandler<MsgHandlerEventArgs> EventHandler { get; set; }
	}
}
