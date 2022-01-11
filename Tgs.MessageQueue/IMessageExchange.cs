using System;
using System.Collections.Generic;

namespace Tgs.MessageQueue;

public interface IMessageExchange
{
	IMessageSender GetMessageSender(IServiceProvider serviceProvider);

	bool Match(string subject, IEnumerable<MessageHeaderValue> header);
}
