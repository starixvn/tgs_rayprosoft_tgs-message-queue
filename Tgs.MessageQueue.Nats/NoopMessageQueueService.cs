using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tgs.MessageQueue.Nats;

internal class NoopMessageQueueService
	: IMessageSender, IMessageReceiver<NatsSubscriptionSettings>, IMessageReceiver<NatsQueueScriptionSettings>
{
	public ValueTask PublishAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header) => ValueTask.CompletedTask;

	public ValueTask<byte[]> RequestAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header) => ValueTask.FromResult(Array.Empty<byte>());

	public ValueTask SendAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header) => ValueTask.CompletedTask;

	public ValueTask<IDisposable> SubscribeAsync(NatsQueueScriptionSettings settings) => ValueTask.FromResult<IDisposable>(null!);

	public ValueTask<IDisposable> SubscribeAsync(NatsSubscriptionSettings settings) => ValueTask.FromResult<IDisposable>(null!);
}
