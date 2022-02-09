namespace Tgs.MessageQueue;

public abstract record Question
{
	public abstract bool CanResponse { get; }
	public abstract bool CanAsk { get; }
	public ReadOnlyMemory<byte> Data { get; protected set; }
	public abstract ValueTask<Answer> AskAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default);
	public abstract ValueTask CompleteAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default);
	public abstract ValueTask FailAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default);
}
