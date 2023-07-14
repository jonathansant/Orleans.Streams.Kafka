using Orleans.Serialization;
using Orleans.Streams.Utils.Serialization;

namespace Orleans.Streams.Kafka.Serialization
{
	public struct SerializationContext
	{
		public Serializer SerializationManager { get; set; }
		public IExternalStreamDeserializer ExternalStreamDeserializer { get; set; }
	}
}
