using DotNet.Globbing;
using Tgs.MessageQueue.Direct.Configuration;

namespace Tgs.MessageQueue.Direct;

internal class DirectMessageExchange : IMessageExchange
{
	private readonly Glob _glob;
	private readonly IEnumerable<ISubscribeRegistration> _subscribeRegistrations;

	public DirectMessageExchange(string pattern, IEnumerable<ISubscribeRegistration> subscribeRegistrations)
	{
		_glob = Glob.Parse(pattern);

		_subscribeRegistrations = subscribeRegistrations ?? throw new ArgumentNullException(nameof(subscribeRegistrations));
	}

	public IMessageSender GetMessageSender(string subject, IServiceProvider serviceProvider)
	{
		foreach (var sub in _subscribeRegistrations)
			if (sub.SubjectGlob.IsMatch(subject))
				return sub.ResolveMessageSender(serviceProvider);

		throw new MessageSenderNotFoundException(subject);
	}

	public bool Match(string subject, IEnumerable<MessageHeaderValue> header)
		=> _glob.IsMatch(subject);
}
