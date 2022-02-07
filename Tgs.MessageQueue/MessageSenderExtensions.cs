namespace Tgs.MessageQueue;

public static class MessageSenderExtensions
{
	public static ValueTask PublishAsync(
		this IMessageSender messageSender,
		string subject,
		ReadOnlyMemory<byte> data,
		CancellationToken cancellationToken = default)
		=> messageSender.PublishAsync(subject, data, Array.Empty<MessageHeaderValue>(), cancellationToken);

	public static ValueTask<ReadOnlyMemory<byte>> RequestAsync(
		this IMessageSender messageSender,
		string subject,
		ReadOnlyMemory<byte> data,
		CancellationToken cancellationToken = default)
		=> messageSender.RequestAsync(subject, data, Array.Empty<MessageHeaderValue>(), cancellationToken);

	public static ValueTask SendAsync(
		this IMessageSender messageSender,
		string subject,
		ReadOnlyMemory<byte> data,
		CancellationToken cancellationToken = default)
		=> messageSender.SendAsync(subject, data, Array.Empty<MessageHeaderValue>(), cancellationToken);
}
