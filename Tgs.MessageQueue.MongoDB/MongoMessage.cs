namespace Tgs.MessageQueue.MongoDB;

internal class MongoMessage
{
	public byte[] Data { get; init; } = default!;

	public string Subject { get; init; } = default!;
}
