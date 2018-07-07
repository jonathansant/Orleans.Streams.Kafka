using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Config
{
	public class KafkaStreamOptions
	{
		public string ExternalMessageIdentifier { get; set; } = "External";
		public IList<string> Topics { get; set; }
		public IList<string> BrokerList { get; set; }
		public bool InternallyManagedQueuesOnly { get; set; } = false;
		public string ConsumerGroupId { get; set; } = "orleans-kafka";
		public double PollTimeout { get; set; } = 100;
	}
}