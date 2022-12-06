using DotNet.Globbing;

namespace Tgs.MessageQueue.InProcess.Configuration;

internal interface ISubscribeRegistration
{
	Glob SubjectGlob { get; }

	IMessageHandlerExecutor CreateExecutor(IServiceProvider serviceProvider);
}
