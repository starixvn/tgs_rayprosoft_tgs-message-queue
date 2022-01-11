using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.Exchanges;

public class AlwaysMatchMessageExchange<TMessageSender> : IMessageExchange
	where TMessageSender : class, IMessageSender
{
	public IMessageSender GetMessageSender(IServiceProvider serviceProvider)
		=> serviceProvider.GetRequiredService<TMessageSender>();

	public bool Match(string subject, IEnumerable<MessageHeaderValue> header) => true;
}
