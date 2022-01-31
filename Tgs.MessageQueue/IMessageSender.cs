namespace Tgs.MessageQueue;

public interface IMessageSender
{
	ValueTask<Answer> AskAsync(string subject, ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);

	ValueTask PublishAsync(string subject, ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);

	ValueTask<ReadOnlyMemory<byte>> RequestAsync(string subject, ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);

	ValueTask SendAsync(string subject, ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);
}
