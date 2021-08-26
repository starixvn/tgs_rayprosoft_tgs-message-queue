using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Tgs.MessageQueue.Nats
{
	public static class NatsMessageSenderExtensions
	{
		public static ValueTask PublishAsync(this IMessageSender messageSender, string subject, byte[] data)
			=> messageSender.PublishAsync(subject, data, Array.Empty<MessageHeaderValue>());

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

		public static ValueTask<byte[]> RequestAsync(this IMessageSender messageSender, string subject, byte[] data)
			=> messageSender.RequestAsync(subject, data, Array.Empty<MessageHeaderValue>());

		public static ValueTask SendAsync(this IMessageSender messageSender, string subject, byte[] data)
			=> messageSender.SendAsync(subject, data, Array.Empty<MessageHeaderValue>());
	}
}
