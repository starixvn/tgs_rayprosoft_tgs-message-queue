using DotNet.Globbing;
using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.Exchanges;

internal class GlobMessageExchange<TMessageSender> : IMessageExchange
	where TMessageSender : class, IMessageSender
{
	private readonly Glob _glob;

	public GlobMessageExchange(string pattern)
	{
		_glob = Glob.Parse(pattern);
	}

	public IMessageSender GetMessageSender(IServiceProvider serviceProvider)
		=> serviceProvider.GetRequiredService<TMessageSender>();

	public bool Match(string subject, IEnumerable<MessageHeaderValue> header)
		=> _glob.IsMatch(subject);
}
