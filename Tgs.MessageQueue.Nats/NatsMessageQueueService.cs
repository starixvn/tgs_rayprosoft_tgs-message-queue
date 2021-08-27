using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NATS.Client;

namespace Tgs.MessageQueue.Nats
{
	internal class NatsMessageQueueService
		: IMessageSender, IMessageReceiver<NatsSubscriptionSettings>, IMessageReceiver<NatsQueueScriptionSettings>
	{
		private readonly IConnection _connection;

		public NatsMessageQueueService(IConnection connection)
		{
			_connection = connection ?? throw new ArgumentNullException(nameof(connection));
		}

		public ValueTask PublishAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		{
			var msg = new Msg(subject, data);

			msg.Header = MakeMsgHeader(header);

			_connection.Publish(msg);

			return ValueTask.CompletedTask;
		}

		public async ValueTask<byte[]> RequestAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		{
			var msg = new Msg(subject, data);

			msg.Header = MakeMsgHeader(header);

			var reply = await _connection.RequestAsync(msg).ConfigureAwait(false);

			if (reply.HasHeaders
				&& reply.Header.GetValues(NatsMessageHeaderValueConsts.FailMessageHeaderValue.Name)?.Length > 0)
				throw new MessageProcessFailException(reply.Data);

			return reply.Data;
		}

		public async ValueTask SendAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		{
			var msg = new Msg(subject, data);

			msg.Header = MakeMsgHeader(header);

			var reply = await _connection.RequestAsync(msg).ConfigureAwait(false);

			if (reply.HasHeaders
				&& reply.Header.GetValues(NatsMessageHeaderValueConsts.FailMessageHeaderValue.Name)?.Length > 0)
				throw new MessageProcessFailException(reply.Data);
		}

		public ValueTask<IDisposable> SubscribeAsync(NatsQueueScriptionSettings settings)
		{
			var subscription = _connection.SubscribeAsync(settings.Subject, settings.Queue, settings.EventHandler);

			return ValueTask.FromResult<IDisposable>(subscription);
		}

		public ValueTask<IDisposable> SubscribeAsync(NatsSubscriptionSettings settings)
		{
			var subscription = _connection.SubscribeAsync(settings.Subject, settings.EventHandler);

			return ValueTask.FromResult<IDisposable>(subscription);
		}

		private MsgHeader MakeMsgHeader(IEnumerable<MessageHeaderValue> header)
		{
			if (header is null)
				throw new ArgumentNullException(nameof(header));

			var msgHeader = new MsgHeader();
			foreach (var headerValue in header)
				msgHeader.Add(headerValue.Name, headerValue.Value);

			return msgHeader;
		}
	}
}
