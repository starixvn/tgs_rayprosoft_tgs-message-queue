namespace Tgs.MessageQueue.RabbitMQ;

internal class NoopMessageQueueService : IMessageSender
{
	public ValueTask<Answer> AskAsync(string subject, ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header)
		=> throw new NotSupportedException();

	public ValueTask PublishAsync(string subject, ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header)
			=> ValueTask.CompletedTask;

	public ValueTask<ReadOnlyMemory<byte>> RequestAsync(string subject, ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header)
		=> ValueTask.FromResult(new ReadOnlyMemory<byte>());

	public ValueTask SendAsync(string subject, ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header)
		=> ValueTask.CompletedTask;
}
