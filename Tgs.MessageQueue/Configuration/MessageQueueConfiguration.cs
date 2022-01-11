using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.Configuration;

public class MessageQueueConfiguration
{
	private readonly List<IMessageExchange> _messageExchanges = new();

	public IServiceCollection Services { get; }

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

	public MessageQueueConfiguration RemoveExchange(IMessageExchange exchange)
	{
		_ = _messageExchanges.Remove(exchange);

		return this;
	}

	internal IEnumerable<IMessageExchange> GetRegisterExchanges() => _messageExchanges;
}
