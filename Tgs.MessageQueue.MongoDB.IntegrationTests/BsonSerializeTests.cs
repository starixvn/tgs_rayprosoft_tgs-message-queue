using ExpectedObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace Tgs.MessageQueue.MongoDB.IntegrationTests;

[TestClass]
public class BsonSerializeTests
{
	[TestMethod]
	public void Serializ_Memory()
	{
		var data = new MongoMessage
		{
			Subject = "test",
			Data = new byte[] { 1, 2, 3 }
		};

		var result = data.ToBson();

		var actual = BsonSerializer.Deserialize<MongoMessage>(result);

		data.ToExpectedObject().ShouldMatch(actual);
	}
}
