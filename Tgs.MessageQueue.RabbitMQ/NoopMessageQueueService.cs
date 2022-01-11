using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tgs.MessageQueue.RabbitMQ;

internal class NoopMessageQueueService : IMessageSender
{
	public ValueTask PublishAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		=> ValueTask.CompletedTask;

	public ValueTask<byte[]> RequestAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		=> ValueTask.FromResult(Array.Empty<byte>());

	public ValueTask SendAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		=> ValueTask.CompletedTask;
}
