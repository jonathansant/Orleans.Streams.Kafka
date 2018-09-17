using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;

namespace Orleans.Streams.Kafka.Serialization
{
	public class BatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly SerializationManager _serializationManager;

		public BatchContainerSerializer(SerializationManager serializationManager)
		{
			_serializationManager = serializationManager;
		}

		public void Dispose()
			=> _serializationManager.Dispose();

		public byte[] Serialize(string topic, KafkaBatchContainer data)
			=> data.ToByteArray(_serializationManager);

		public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
			=> config;
	}
}
