using Confluent.Kafka;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using System.Text;

namespace Orleans.Streams.Kafka.Serialization
{
	internal class KafkaBatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly OrleansJsonSerializer _serializer;

		public KafkaBatchContainerSerializer(OrleansJsonSerializer serializer)
		{
			_serializer = serializer;
		}

		public byte[] Serialize(KafkaBatchContainer data, Confluent.Kafka.SerializationContext context)
		{
			var serializedString = _serializer.Serialize(data, typeof(KafkaBatchContainer));
			var bytes = new byte[serializedString.Length];
			Encoding.UTF8.GetBytes(serializedString, bytes);
			return bytes;
		}
	}
}