using System.Collections;
using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Config
{
	public static class KafkaStreamOptionsExtensions
	{
		public static IDictionary<string, object> ToProducerProperties(this KafkaStreamOptions options)
		{
			return new Dictionary<string, object> 
			{ 
				{ "bootstrap.servers", string.Join(",", options.BrokerList) } 
			};
		} 
	}
}