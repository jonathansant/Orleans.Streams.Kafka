using Orleans.Serialization;
using Orleans.Streams.Utils.Serialization;

namespace Orleans.Streams.Kafka.Serialization
{
	public struct SerializationContext
	{
		public OrleansJsonSerializer SerializationManager { get; set; }
		public IExternalStreamDeserializer ExternalStreamDeserializer { get; set; }
	}
}
