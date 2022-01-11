using DotNet.Globbing;
using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.MongoDB.Exchanges;

internal class MongoDBGlobMessageExchange : IMessageExchange
{
	private readonly Glob _glob;
	private readonly string _managerName;
	private readonly string _queueName;

	public MongoDBGlobMessageExchange(string pattern, string managerName, string queueName)
	{
		if (string.IsNullOrWhiteSpace(managerName))
			throw new ArgumentException($"'{nameof(managerName)}' 不得為 Null 或空白字元。", nameof(managerName));
		if (string.IsNullOrWhiteSpace(queueName))
			throw new ArgumentException($"'{nameof(queueName)}' 不得為 Null 或空白字元。", nameof(queueName));

		_glob = Glob.Parse(pattern);
		_managerName = managerName;
		_queueName = queueName;
	}

	public IMessageSender GetMessageSender(IServiceProvider serviceProvider)
	{
		var queueManagerService = serviceProvider.GetRequiredService<QueueManagerService>();

		var queueManager = queueManagerService[_managerName];

		return new MongoDbMessageQueueService(queueManager, _queueName);
	}

	public bool Match(string subject, IEnumerable<MessageHeaderValue> header) => _glob.IsMatch(subject);
}
