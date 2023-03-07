using DotNet.Globbing;
using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.RabbitMQ.Exchanges;

internal class RabbitGlobMessageExchange : IMessageExchange
{
	private readonly string _exchangeName;
	private readonly Glob _glob;

	public RabbitGlobMessageExchange(string pattern, string exchangeName)
	{
		if (string.IsNullOrWhiteSpace(exchangeName))
			throw new ArgumentException($"'{nameof(exchangeName)}' 不得為 Null 或空白字元。", nameof(exchangeName));

		_glob = Glob.Parse(pattern);
		_exchangeName = exchangeName;
	}

	public IMessageSender GetMessageSender(string subject, IServiceProvider serviceProvider)
	{
		var factory = serviceProvider.GetRequiredService<IMessageQueueServiceFactory>();
		var connectionManager = serviceProvider.GetRequiredService<RabbitMQConnectionManager>();

		return factory.CreateMessageQueueService(
			 serviceProvider,
			 _exchangeName,
			 connectionManager.MessageQueueChannel);
	}

	public bool Match(string subject, IEnumerable<MessageHeaderValue> header) => _glob.IsMatch(subject);
}
