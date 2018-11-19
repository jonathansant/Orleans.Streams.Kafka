using System;
using Orleans.Streams.Kafka.Utils;
using System.Collections.Generic;
using System.IO;

namespace Orleans.Streams.Kafka.Config
{
	internal static class KafkaStreamOptionsExtensions
	{
		public static IDictionary<string, string> ToProducerProperties(this KafkaStreamOptions options)
			=> CreateCommonProperties(options);

		public static IDictionary<string, string> ToConsumerProperties(this KafkaStreamOptions options)
		{
			var config = CreateCommonProperties(options);

			config.TryAdd("group.id", options.ConsumerGroupId);
			config.TryAdd("enable.auto.commit", "false");

			return config;
		}

		public static IDictionary<string, string> ToAdminProperties(this KafkaStreamOptions options)
			=> CreateCommonProperties(options);

		private static IDictionary<string, string> CreateCommonProperties(KafkaStreamOptions options)
		{
			var config = new Dictionary<string, string>
			{
				{ "bootstrap.servers", string.Join(",", options.BrokerList) }
			};

			config.TryAdd("api.version.request", options.ApiVersionRequest.ToString());
			config.TryAdd("broker.version.fallback", options.BrokerVersionFallback);
			config.TryAdd("api.version.fallback.ms", options.ApiVersionFallbackMs.ToString());
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
		public static KafkaStreamOptions WithSaslOptions(
			this KafkaStreamOptions options, 
			Credentials credentials, 
			string saslMechanism = "PLAIN"
		)
		{
			options.SaslMechanisms = saslMechanism;
			options.SecurityProtocol = "SASL_SSL";
			options.SaslUserName = credentials.UserName;
			options.SaslPassword = credentials.Password;
			options.SslCaLocation = credentials.SslCaLocation;

			return options;
		}
	}
}