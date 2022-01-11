using System;
using System.Threading.Tasks;

namespace Tgs.MessageQueue;

public static class MessageSenderExtensions
{
	public static ValueTask PublishAsync(this IMessageSender messageSender, string subject, byte[] data)
		=> messageSender.PublishAsync(subject, data, Array.Empty<MessageHeaderValue>());

	public static ValueTask<byte[]> RequestAsync(this IMessageSender messageSender, string subject, byte[] data)
		=> messageSender.RequestAsync(subject, data, Array.Empty<MessageHeaderValue>());

	public static ValueTask SendAsync(this IMessageSender messageSender, string subject, byte[] data)
		=> messageSender.SendAsync(subject, data, Array.Empty<MessageHeaderValue>());
}
