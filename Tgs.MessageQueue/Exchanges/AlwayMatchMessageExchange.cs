using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.Exchanges
{
	public class AlwayMatchMessageExchange<TMessageSender> : IMessageExchange
		where TMessageSender : class, IMessageSender
	{
		public IMessageSender GetMessageSender(IServiceProvider serviceProvider)
			=> serviceProvider.GetRequiredService<TMessageSender>();

		public bool Match(string subject, IEnumerable<MessageHeaderValue> header) => true;
	}
}
