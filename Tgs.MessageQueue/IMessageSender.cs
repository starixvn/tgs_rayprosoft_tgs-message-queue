namespace Tgs.MessageQueue;

public interface IMessageSender
{
	ValueTask PublishAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header);

	ValueTask<byte[]> RequestAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header);

	ValueTask SendAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header);
}
