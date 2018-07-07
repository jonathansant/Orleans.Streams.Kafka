using System.Collections;
using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Config
{
	internal static class KafkaStreamOptionsExtensions
	{
		public static IDictionary<string, object> ToProducerProperties(this KafkaStreamOptions options)
		{
			return new Dictionary<string, object> 
			{ 
				{ "bootstrap.servers", string.Join(",", options.BrokerList) } 
			};
		} 
		
		public static IDictionary<string, object> ToConsumerProperties(this KafkaStreamOptions options)
		{
			return new Dictionary<string, object> 
			{ 
				{ "group.id", options.ConsumerGroupId },
				{ "bootstrap.servers", string.Join(",", options.BrokerList) },
				{ "enable.auto.commit", false },
				//{ "auto.commit.interval.ms", 5000 },
				//{ "auto.offset.reset", "earliest" }
			};
		} 
	}
}