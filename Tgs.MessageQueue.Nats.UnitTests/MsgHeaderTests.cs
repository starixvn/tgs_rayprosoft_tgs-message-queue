using NATS.Client;
using NUnit.Framework;

namespace Tgs.MessageQueue.Nats.UnitTests;

public class MsgHeaderTests
{
	[Test]
	public void Msg_Check_Header_Has_Value()
	{
		var sut = new Msg
		{
			Header = new MsgHeader()
		};

		var headerValue = NatsMessageHeaderValueConsts.FailMessageHeaderValue;

		sut.Header.Add(headerValue.Name, headerValue.Value);

		Assert.IsTrue(sut.HasHeaders);
	}

	[Test]
	public void MsgHeader_Not_Set_Fail_Should_Identify()
	{
		var sut = new MsgHeader();

		var actual = sut.GetValues(NatsMessageHeaderValueConsts.FailMessageHeaderValue.Name);

		Assert.IsFalse(actual?.Length > 0);
	}

	[Test]
	public void MsgHeader_Set_Fail_Should_Identify()
	{
		var sut = new MsgHeader();

		var headerValue = NatsMessageHeaderValueConsts.FailMessageHeaderValue;

		sut.Add(headerValue.Name, headerValue.Value);

		var actual = sut.GetValues(headerValue.Name);

		Assert.IsTrue(actual?.Length > 0);
	}
}
