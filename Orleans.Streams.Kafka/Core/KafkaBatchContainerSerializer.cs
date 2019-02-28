using Confluent.Kafka;
using Orleans.Serialization;

namespace Orleans.Streams.Kafka.Core
{
	internal class KafkaBatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly SerializationManager _serializationManager;

		public KafkaBatchContainerSerializer(SerializationManager serializationManager)
		{
			_serializationManager = serializationManager;
		}

		public byte[] Serialize(
			KafkaBatchContainer data,
			bool isKey,
			MessageMetadata messageMetadata,
			TopicPartition destination
		)
			=> _serializationManager.SerializeToByteArray(data);
	}
}