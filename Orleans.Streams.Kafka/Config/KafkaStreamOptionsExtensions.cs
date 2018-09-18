using System.Collections.Generic;
using Orleans.Streams.Kafka.Extensions;

namespace Orleans.Streams.Kafka.Config
{
	internal static class KafkaStreamOptionsExtensions
	{
		public static IDictionary<string, object> ToProducerProperties(this KafkaStreamOptions options)
			=> CreateCommonProperties(options);

		public static IDictionary<string, object> ToConsumerProperties(this KafkaStreamOptions options)
		{
			var config = CreateCommonProperties(options);

			config.TryAdd("group.id", options.ConsumerGroupId);
			config.TryAdd("enable.auto.commit", false);

			return config;
		}

		public static IDictionary<string, object> ToAdminProperties(this KafkaStreamOptions options)
			=> CreateCommonProperties(options);

		private static IDictionary<string, object> CreateCommonProperties(KafkaStreamOptions options)
		{
			var config = new Dictionary<string, object>
			{
				{ "bootstrap.servers", string.Join(",", options.BrokerList) }
			};

			config.TryAdd("api.version.request", options.ApiVersionRequest);
			config.TryAdd("broker.version.fallback", options.BrokerVersionFallback);
			config.TryAdd("api.version.fallback.ms", options.ApiVersionFallbackMs);
			config.TryAdd("sasl.mechanisms", options.SaslMechanisms);
			config.TryAdd("security.protocol", options.SecurityProtocol);
			config.TryAdd("ssl.ca.location", options.SslCaLocation);
			config.TryAdd("sasl.username", options.SaslUserName);
			config.TryAdd("sasl.password", options.SaslPassword);

			return config;
		}
	}

	public static class KafkaStreamOptionsPublicExtensions
	{
		public static KafkaStreamOptions WithConfluentCloudOptions(this KafkaStreamOptions options, Credentials credentials)
		{
			options.ApiVersionRequest = true;
			options.BrokerVersionFallback = "0.10.0.0";
			options.ApiVersionFallbackMs = 0;
			options.SaslMechanisms = "PLAIN";
			options.SecurityProtocol = "SASL_SSL";
			options.SaslUserName = credentials.UserName;
			options.SaslPassword = credentials.Password;
			options.SslCaLocation = credentials.SslCaLocation;

			return options;
		}
	}
}