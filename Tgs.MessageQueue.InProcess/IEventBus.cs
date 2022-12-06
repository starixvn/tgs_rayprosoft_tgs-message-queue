namespace Tgs.MessageQueue.InProcess;

internal interface IEventBus
{
	IObservable<InProcessMessage> MessageObservable { get; }
}
