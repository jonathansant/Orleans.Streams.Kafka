using Confluent.Kafka;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using SerializationContext = Confluent.Kafka.SerializationContext;

namespace Orleans.Streams.Kafka.Serialization
{
	internal class KafkaBatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly SerializationManager _serializationManager;

		public KafkaBatchContainerSerializer(SerializationManager serializationManager)
		{
			_serializationManager = serializationManager;
		}

		public byte[] Serialize(KafkaBatchContainer data, SerializationContext context)
			=> _serializationManager.SerializeToByteArray(data);
	}
}