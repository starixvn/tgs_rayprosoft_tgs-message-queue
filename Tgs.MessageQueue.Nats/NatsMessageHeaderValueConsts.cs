namespace Tgs.MessageQueue.Nats;

public static class NatsMessageHeaderValueConsts
{
	public static readonly MessageHeaderValue FailMessageHeaderValue = new("Nats_Fail", string.Empty);
	public static readonly string SessionAskKey = "Nats_Ask_Id";
	public static readonly string SessionReplyKey = "Nats_Reply_Id";
	public static readonly string SessionReplySubjectKey = "Nats_Reply";
}
