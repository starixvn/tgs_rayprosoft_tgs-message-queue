namespace Tgs.MessageQueue;

public abstract record Answer
{
	public abstract bool CanResponse { get; }
	public abstract bool CanAsk { get; }
	public ReadOnlyMemory<byte> Result { get; protected set; }
	public abstract ValueTask<Answer> AskAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default);
	public abstract ValueTask CompleteAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default);
	public abstract ValueTask FailAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default);
}
