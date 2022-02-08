namespace Tgs.MessageQueue.Nats;

internal record NatsQuestion : Question
{
	private readonly string _replySubject;
	private readonly IMessageSender _messageSender;

	public NatsQuestion(ReadOnlyMemory<byte> data, string replySubject, IMessageSender messageSender)
	{
		Data = data;
		_replySubject = replySubject;
		_messageSender = messageSender ?? throw new ArgumentNullException(nameof(messageSender));
	}

	public override ValueTask<Answer> AskAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> _messageSender.AskAsync(_replySubject, data, header, cancellationToken);

	public override ValueTask CompleteAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> _messageSender.PublishAsync(_replySubject, data, header, cancellationToken);

	public override ValueTask FailAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> _messageSender.PublishFailAsync(_replySubject, data, header, cancellationToken: cancellationToken);
}
