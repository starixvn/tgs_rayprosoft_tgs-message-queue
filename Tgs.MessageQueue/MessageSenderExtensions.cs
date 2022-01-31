namespace Tgs.MessageQueue;

public static class MessageSenderExtensions
{
	public static ValueTask PublishAsync(this IMessageSender messageSender, string subject, ReadOnlyMemory<byte> data)
		=> messageSender.PublishAsync(subject, data, Array.Empty<MessageHeaderValue>());

	public static ValueTask<ReadOnlyMemory<byte>> RequestAsync(this IMessageSender messageSender, string subject, ReadOnlyMemory<byte> data)
		=> messageSender.RequestAsync(subject, data, Array.Empty<MessageHeaderValue>());

	public static ValueTask SendAsync(this IMessageSender messageSender, string subject, ReadOnlyMemory<byte> data)
		=> messageSender.SendAsync(subject, data, Array.Empty<MessageHeaderValue>());
}
