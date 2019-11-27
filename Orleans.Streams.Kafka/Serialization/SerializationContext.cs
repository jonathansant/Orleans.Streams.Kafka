using Orleans.Serialization;
using Orleans.Streams.Utils.Serialization;

namespace Orleans.Streams.Kafka.Serialization
{
	public struct SerializationContext
	{
		public SerializationManager SerializationManager { get; set; }
		public IExternalStreamSerDes ExternalStreamDeserializer { get; set; }
	}
}
