using Confluent.Kafka;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using System;
using System.Collections.Immutable;
using System.Text;

namespace Orleans.Streams.Kafka.Serialization
{
	internal class KafkaBatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly OrleansJsonSerializer _serializationManager;

		public KafkaBatchContainerSerializer(OrleansJsonSerializer serializationManager)
		{
			_serializationManager = serializationManager;
		}

		public byte[] Serialize(KafkaBatchContainer data, Confluent.Kafka.SerializationContext context)
		{
			var serializedString = _serializationManager.Serialize(data, typeof(KafkaBatchContainer));
			var bytes = new byte[serializedString.Length];
			Encoding.UTF8.GetBytes(serializedString, bytes);
			return bytes;
		}
	}
}