using Confluent.Kafka;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;

namespace Orleans.Streams.Kafka.Serialization
{
	internal class KafkaBatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly Serializer _serializer;

		public KafkaBatchContainerSerializer(Serializer serializer)
		{
			_serializer = serializer;
		}

		public byte[] Serialize(KafkaBatchContainer data, Confluent.Kafka.SerializationContext context)
		{
			// todo: check
			byte[] result = new byte[100];
			_serializer.Serialize(data, result);
			return result;
		}
	}
}