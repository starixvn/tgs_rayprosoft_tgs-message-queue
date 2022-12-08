using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.Configuration;

public class MessageQueueConfiguration
{
	private readonly List<IMessageExchange> _messageExchanges = new();

	public IServiceCollection Services { get; }

	public string? SessionReplySubject { get; private set; }

	public MessageQueueConfiguration(IServiceCollection services)
	{
		Services = services ?? throw new ArgumentNullException(nameof(services));
	}

	public MessageQueueConfiguration AddExchange(IMessageExchange exchange)
	{
		_messageExchanges.Add(exchange);

		return this;
	}

	public MessageQueueConfiguration ClearAllExchanges()
	{
		_messageExchanges.Clear();

		return this;
	}

	public MessageQueueConfiguration PushExchange(IMessageExchange exchange)
	{
		_messageExchanges.Insert(0, exchange);

		return this;
	}

	public MessageQueueConfiguration RegisterSessionReplySubject(string subject)
	{
		if (string.IsNullOrWhiteSpace(subject))
			throw new ArgumentException($"'{nameof(subject)}' 不得為 Null 或空白字元。", nameof(subject));
		SessionReplySubject = subject;

		return this;
	}

	public MessageQueueConfiguration RemoveExchange(IMessageExchange exchange)
	{
		_ = _messageExchanges.Remove(exchange);

		return this;
	}

	internal IEnumerable<IMessageExchange> GetRegisterExchanges() => _messageExchanges;
}
