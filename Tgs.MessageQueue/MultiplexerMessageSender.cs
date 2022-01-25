using System.Diagnostics;

namespace Tgs.MessageQueue;

internal class MultiplexerMessageSender : IMessageSender, IMessageExchange
{
	private static readonly ActivitySource _SenderActivitySource = new($"Tgs.MessageQueue.{nameof(MultiplexerMessageSender)}");

	private readonly IMessageExchange[] _exchanges = Array.Empty<IMessageExchange>();
	private readonly IServiceProvider _serviceProvider;

	public MultiplexerMessageSender(IServiceProvider serviceProvider, IEnumerable<IMessageExchange> exchanges)
	{
		_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
		_exchanges = (exchanges ?? throw new ArgumentNullException(nameof(exchanges))).ToArray();
	}

	public IMessageSender GetMessageSender(IServiceProvider serviceProvider) => this;

	public bool Match(string subject, IEnumerable<MessageHeaderValue> header) => true;

	public ValueTask PublishAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
	{
		using var activity = _SenderActivitySource.StartActivity($"{nameof(MultiplexerMessageSender)}.{nameof(PublishAsync)}");
		_ = (activity?.AddTag("subject", subject));

		var sender = GetMessageSender(subject, header);

		return sender.PublishAsync(subject, data, header);
	}

	public ValueTask<byte[]> RequestAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
	{
		using var activity = _SenderActivitySource.StartActivity($"{nameof(MultiplexerMessageSender)}.{nameof(RequestAsync)}");
		_ = (activity?.AddTag("subject", subject));

		var sender = GetMessageSender(subject, header);

		return sender.RequestAsync(subject, data, header);
	}

	public ValueTask SendAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
	{
		using var activity = _SenderActivitySource.StartActivity($"{nameof(MultiplexerMessageSender)}.{nameof(SendAsync)}");
		_ = (activity?.AddTag("subject", subject));

		var sender = GetMessageSender(subject, header);

		return sender.SendAsync(subject, data, header);
	}

	private IMessageSender GetMessageSender(string subject, IEnumerable<MessageHeaderValue> header)
	{
		using var activity = _SenderActivitySource.StartActivity($"{nameof(MultiplexerMessageSender)}.{nameof(GetMessageSender)}");
		_ = (activity?.AddTag("subject", subject));

		foreach (var exchange in _exchanges)
			if (exchange.Match(subject, header))
				return exchange.GetMessageSender(_serviceProvider);

		throw new MessageSenderNotFoundException(subject);
	}
}
