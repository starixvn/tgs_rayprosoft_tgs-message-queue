namespace Tgs.MessageQueue.Nats;

internal record NatsAnswer : Answer
{
	private readonly string _replySubject;
	private readonly IMessageSender _messageSender;
	public override bool CanResponse => !string.IsNullOrEmpty(_replySubject);

	internal NatsAnswer(ReadOnlyMemory<byte> responseData, string replySubject, IMessageSender messageSender)
	{
		Result = responseData;
		_replySubject = replySubject;
		_messageSender = messageSender ?? throw new ArgumentNullException(nameof(messageSender));
	}

	public override ValueTask<Answer> AskAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanResponse
			? _messageSender.AskAsync(_replySubject, data, header, cancellationToken)
			: throw new NatsReplySubjectNullException();

	public override ValueTask CompleteAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanResponse
			? _messageSender.PublishAsync(_replySubject, data, header, cancellationToken)
			: throw new NatsReplySubjectNullException();

	public override ValueTask FailAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanResponse
			? _messageSender.PublishFailAsync(_replySubject, data, header, cancellationToken: cancellationToken)
			: throw new NatsReplySubjectNullException();
}
