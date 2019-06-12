using System;
using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Config
{
	public class KafkaStreamOptions
	{
		public string ExternalMessageIdentifier { get; set; } = "external";
		public IList<Topic> Topics { get; set; }
		public IList<string> BrokerList { get; set; }
		public string ConsumerGroupId { get; set; } = "orleans-kafka";
		public TimeSpan PollTimeout { get; set; } = TimeSpan.FromMilliseconds(100);
		public TimeSpan AdminRequestTimeout { get; set; } = TimeSpan.FromSeconds(5);
		public ConsumeMode ConsumeMode { get; set; } = ConsumeMode.LastCommittedMessage;
		public TimeSpan ProducerTimeout { get; set; } = TimeSpan.FromSeconds(5);
		public bool ApiVersionRequest { get; set; } = true;
		public string BrokerVersionFallback { get; set; } = "0.10.0.0";
		public int ApiVersionFallbackMs { get; set; }
		public string SecurityProtocol { get; set; }
		public string SslCaLocation { get; set; }
		public string SaslUserName { get; set; }
		public string SaslPassword { get; set; }
		public string SaslMechanisms { get; set; }
		public TimeSpan PollBufferTimeout { get; set; } = TimeSpan.FromMilliseconds(500);
		public bool MessageTrackingEnabled { get; set; }
	}

	public class Credentials
	{
		public string UserName { get; set; }
		public string Password { get; set; }
		public string SslCaLocation { get; set; }
	}

	public class Topic
	{
		public string Name { get; set; }
		public bool IsExternal { get; set; }
	}

	public enum ConsumeMode
	{
		StreamStart = 0,
		LastCommittedMessage = 1,
		StreamEnd = 2
	}
}