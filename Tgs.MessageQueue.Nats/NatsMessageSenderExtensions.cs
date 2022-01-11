namespace Tgs.MessageQueue.Nats;

public static class NatsMessageSenderExtensions
{
	public static ValueTask PublishFailAsync(this IMessageSender messageSender, string subject, byte[] data)
				=> messageSender.PublishAsync(
			subject,
			data,
			new[] { NatsMessageHeaderValueConsts.FailMessageHeaderValue });

	public static ValueTask PublishFailAsync(this IMessageSender messageSender, string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		=> messageSender.PublishAsync(
			subject,
			data,
			header.Concat(new[] { NatsMessageHeaderValueConsts.FailMessageHeaderValue }));
}
