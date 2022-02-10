using System.Collections.Concurrent;
using Tgs.MessageQueue.Nats;

namespace Tgs.MessageQueue;

internal class ReplyPromiseStore : IReplyPromiseStore
{
	private readonly ConcurrentDictionary<Guid, TaskCompletionSource<Answer>> _store = new();

	public (Guid Id, Task<Answer> Promise) CreatePromise(CancellationToken cancellationToken)
	{
		var tcs = new TaskCompletionSource<Answer>();
		var id = Guid.NewGuid();

		if (!_store.TryAdd(id, tcs))
			throw new ReplyNotStoreException();

		_ = cancellationToken.Register(() => SetCanceled(id));

		return (id, tcs.Task);
	}

	public void SetCanceled(Guid id)
	{
		if (_store.TryGetValue(id, out var tcs))
		{
			_ = tcs.TrySetCanceled();
			_ = _store.TryRemove(id, out _);
		}
	}

	public void SetException(Guid id, Exception ex)
	{
		if (_store.TryGetValue(id, out var tcs))
		{
			_ = tcs.TrySetException(ex);
			_ = _store.TryRemove(id, out _);
		}
	}

	public void SetResult(Guid id, Answer answer)
	{
		if (_store.TryGetValue(id, out var tcs))
		{
			_ = tcs.TrySetResult(answer);
			_ = _store.TryRemove(id, out _);
		}
	}
}
