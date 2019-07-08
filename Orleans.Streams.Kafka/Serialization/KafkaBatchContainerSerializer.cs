using Confluent.Kafka;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;

namespace Orleans.Streams.Kafka.Serialization
{
	internal class KafkaBatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly SerializationManager _serializationManager;

		public KafkaBatchContainerSerializer(SerializationManager serializationManager)
		{
			_serializationManager = serializationManager;
		}

		public byte[] Serialize(KafkaBatchContainer data, Confluent.Kafka.SerializationContext context)
			=> _serializationManager.SerializeToByteArray(data);
	}
}