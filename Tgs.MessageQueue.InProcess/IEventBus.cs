namespace Tgs.MessageQueue.InProcess;

internal interface IEventBus
{
	IObservable<InProcessMessage> BucketChangedObservable { get; }
}
