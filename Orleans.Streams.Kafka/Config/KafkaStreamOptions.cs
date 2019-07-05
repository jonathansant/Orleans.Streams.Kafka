using System;
using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Config
{
	public class KafkaStreamOptions
	{
		public IList<TopicConfig> Topics { get; set; } = new List<TopicConfig>();
		public IList<string> BrokerList { get; set; }
		public string ConsumerGroupId { get; set; } = "orleans-kafka";
		public TimeSpan PollTimeout { get; set; } = TimeSpan.FromMilliseconds(100);
		public TimeSpan AdminRequestTimeout { get; set; } = TimeSpan.FromSeconds(5);
		public ConsumeMode ConsumeMode { get; set; } = ConsumeMode.LastCommittedMessage;
		public TimeSpan ProducerTimeout { get; set; } = TimeSpan.FromSeconds(5);
		public bool ApiVersionRequest { get; set; } = true;
		public string BrokerVersionFallback { get; set; } = "0.10.0.0";
		public int? ApiVersionFallbackMs { get; set; }
		public SecurityProtocol SecurityProtocol { get; set; }
		public string SslCaLocation { get; set; }
		public string SaslUserName { get; set; }
		public string SaslPassword { get; set; }
		public SaslMechanism SaslMechanism { get; set; }
		public TimeSpan PollBufferTimeout { get; set; } = TimeSpan.FromMilliseconds(500);
		public bool MessageTrackingEnabled { get; set; }

		/// <summary>
		/// Add a new internal topic.
		/// </summary>
		/// <param name="name">Topic Name</param>
		public KafkaStreamOptions AddTopic(string name)
		{
			Topics.Add(new TopicConfig
			{
				IsExternal = false,
				Name = name
			});

			return this;
		}

		/// <summary>
		/// Add a new external topic.
		/// </summary>
		/// <param name="name">Topic Name</param>
		public KafkaStreamOptions AddExternalTopic(string name)
		{
			Topics.Add(new TopicConfig
			{
				IsExternal = true,
				Name = name,
			});

			return this;
		}
	}

	public class Credentials
	{
		public string UserName { get; set; }
		public string Password { get; set; }
		public string SslCaLocation { get; set; }
	}

	public class TopicConfig
	{
		public string Name { get; set; }

		/// <summary>
		/// Specifies whether the topic will be produced by producers external to the silo
		/// </summary>
		public bool IsExternal { get; set; }
	}

	public enum ConsumeMode
	{
		StreamStart = 0,
		LastCommittedMessage = 1,
		StreamEnd = 2
	}

	public enum SaslMechanism
	{
		Gssapi,
		Plain,
		ScramSha256,
		ScramSha512,
	}

	public enum SecurityProtocol
	{
		Plaintext,
		Ssl,
		SaslPlaintext,
		SaslSsl,
	}
}