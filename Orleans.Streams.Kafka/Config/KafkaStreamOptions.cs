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
		/// <param name="topicCreationConfig"></param>
		public KafkaStreamOptions AddTopic(string name, TopicCreationConfig topicCreationConfig = null)
		{
			var config = new TopicConfig { IsExternal = false, Name = name };
			if (topicCreationConfig != null)
			{
				config.AutoCreate = topicCreationConfig.AutoCreate;
				config.Partitions = topicCreationConfig.Partitions;
				config.ReplicationFactor = topicCreationConfig.ReplicationFactor;
				config.RetentionPeriodInMs = topicCreationConfig.RetentionPeriodInMs;
			}

			Topics.Add(config);

			return this;
		}

		/// <summary>
		/// Add a new external topic.
		/// </summary>
		/// <param name="name">Topic Name</param>
		/// <param name="topicCreationConfig"></param>
		public KafkaStreamOptions AddExternalTopic<T>(string name, TopicCreationConfig topicCreationConfig = null)
		{
			AddExternalTopic(typeof(T), name, topicCreationConfig);
			return this;
		}

		/// <summary>
		/// Add a new external topic.
		/// </summary>
		/// <param name="type">The data type that this contract will use.</param>
		/// <param name="name">Topic Name</param>
		/// <param name="topicCreationConfig"></param>
		public KafkaStreamOptions AddExternalTopic(
			Type type,
			string name,
			TopicCreationConfig topicCreationConfig = null
		)
		{
			var config = new TopicConfig { IsExternal = true, Name = name, ExternalContractType = type };
			if (topicCreationConfig != null)
			{
				config.AutoCreate = topicCreationConfig.AutoCreate;
				config.Partitions = topicCreationConfig.Partitions;
				config.ReplicationFactor = topicCreationConfig.ReplicationFactor;
			}

			Topics.Add(config);

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

		/// <summary>
		/// The expected DataType that is expected on this topic
		/// </summary>
		public Type ExternalContractType { get; set; }

		/// <summary>
		/// Determines whether the topic will be auto created
		/// </summary>
		/// <remarks><c>false</c> by default.</remarks>
		public bool AutoCreate { get; set; }

		/// <summary>
		/// If <see cref="AutoCreate"/> is true the topic will
		/// be created with set number of topics
		/// </summary>
		/// <remarks>-1 by default</remarks>
		public int Partitions { get; set; } = -1;

		/// <summary>
		/// If <see cref="AutoCreate"/> is true the topic will
		/// be created with the Replication Factor defined
		/// </summary>
		/// <remarks>1 by default</remarks>
		public short ReplicationFactor { get; set; } = 1;

		/// <summary>
		/// If <see cref="RetentionPeriodInMs"/> is set the topic will
		/// be created with only retain data for this much duration.
		/// If not set, it'll take default configuration of broker which is 7 days.
		/// </summary>
		/// <remarks>7 days by default</remarks>
		public ulong RetentionPeriodInMs { get; set; } = 604800000;
	}

	public class TopicCreationConfig
	{
		/// <summary>
		/// Determines whether the topic will be auto created
		/// </summary>
		/// <remarks><c>false</c> by default.</remarks>
		public bool AutoCreate { get; set; }

		/// <summary>
		/// If <see cref="AutoCreate"/> is true the topic will
		/// be created with set number of topics
		/// </summary>
		/// <remarks>-1 by default</remarks>
		public int Partitions { get; set; } = -1;

		/// <summary>
		/// If <see cref="AutoCreate"/> is true the topic will
		/// be created with the Replication Factor defined
		/// </summary>
		/// <remarks>1 by default</remarks>
		public short ReplicationFactor { get; set; } = 1;

		/// <summary>
		/// If <see cref="RetentionPeriodInMs"/> is set the topic will
		/// be created with only retain data for this much duration in milliseconds.
		/// If not set, it'll take default configuration of broker which is 7 days.
		/// </summary>
		/// <remarks>7 days by default</remarks>
		public ulong RetentionPeriodInMs { get; set; } = 604800000;
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