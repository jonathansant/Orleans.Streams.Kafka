using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Orleans.Streams.Kafka.Utils;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Concurrent;

namespace Orleans.Streams.Kafka.Serialization
{
	public class AvroExternalStreamDeserializer : IExternalStreamDeserializer
	{
		private readonly ConcurrentDictionary<Type, object> _deserializers;
		private readonly ISchemaRegistryClient _registry;

		public AvroExternalStreamDeserializer(ISchemaRegistryClient schemaRegistry)
		{
			_registry = schemaRegistry;
			_deserializers = new ConcurrentDictionary<Type, object>();
		}

		public T Deserialize<T>(QueueProperties queueProps, byte[] data)
		{
			var deserializer = GetOrCreate<T>();
			return AsyncHelper.RunSync(() => deserializer.DeserializeAsync(
				new ReadOnlyMemory<byte>(data),
				false,
				new Confluent.Kafka.SerializationContext(MessageComponentType.Value, queueProps.QueueName)
			));
		}

		public void Dispose()
			=> _registry.Dispose();

		private AvroDeserializer<T> GetOrCreate<T>()
			=> (AvroDeserializer<T>)_deserializers.GetOrAdd(
				typeof(T),
				type => new AvroDeserializer<T>(_registry)
			);
	}
}
