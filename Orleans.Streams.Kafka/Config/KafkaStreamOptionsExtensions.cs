using Confluent.Kafka;

namespace Orleans.Streams.Kafka.Config
{
	internal static class KafkaStreamOptionsExtensions
	{
		public static ProducerConfig ToProducerProperties(this KafkaStreamOptions options)
		{
			var config = CreateCommonProperties<ProducerConfig>(options);
			config.MessageTimeoutMs = (int)options.ProducerTimeout.TotalMilliseconds;

			return config;
		}

		public static ConsumerConfig ToConsumerProperties(this KafkaStreamOptions options)
		{
			var config = CreateCommonProperties<ConsumerConfig>(options);

			config.GroupId = options.ConsumerGroupId;
			config.EnableAutoCommit = false;

			return config;
		}

		public static AdminClientConfig ToAdminProperties(this KafkaStreamOptions options)
			=> CreateCommonProperties<AdminClientConfig>(options);

		private static TClientConfig CreateCommonProperties<TClientConfig>(KafkaStreamOptions options)
			where TClientConfig : ClientConfig, new()
			=> new TClientConfig
			{
				BootstrapServers = string.Join(",", options.BrokerList),
				BrokerVersionFallback = options.BrokerVersionFallback,
				ApiVersionRequest = options.ApiVersionRequest,
				ApiVersionRequestTimeoutMs = options.ApiVersionFallbackMs,
				SaslMechanism = (Confluent.Kafka.SaslMechanism)(int)options.SaslMechanism,
				SecurityProtocol = (Confluent.Kafka.SecurityProtocol)(int)options.SecurityProtocol,
				SslCaLocation = options.SslCaLocation,
				SaslUsername = options.SaslUserName,
				SaslPassword = options.SaslPassword
			};
	}

	public static class KafkaStreamOptionsPublicExtensions
	{
		public static KafkaStreamOptions WithSaslOptions(
			this KafkaStreamOptions options,
			Credentials credentials,
			SaslMechanism saslMechanism = SaslMechanism.Plain
		)
		{
			options.SaslMechanism = saslMechanism;
			options.SecurityProtocol = SecurityProtocol.SaslSsl;
			options.SaslUserName = credentials.UserName;
			options.SaslPassword = credentials.Password;
			options.SslCaLocation = credentials.SslCaLocation;

			return options;
		}
	}
}