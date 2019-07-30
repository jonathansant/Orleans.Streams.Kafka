using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Orleans.Streams.Kafka.Utils;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Serialization
{
	public class AvroExternalStreamDeserializer : IExternalStreamDeserializer
	{
		private readonly ConcurrentDictionary<Type, AvroDeserializerWrapper> _deserializers;
		private readonly ISchemaRegistryClient _registry;
		private readonly MethodInfo _getOrCreateMethodInfo;

		public AvroExternalStreamDeserializer(ISchemaRegistryClient schemaRegistry)
		{
			_registry = schemaRegistry;
			_deserializers = new ConcurrentDictionary<Type, AvroDeserializerWrapper>();
			_getOrCreateMethodInfo = typeof(AvroExternalStreamDeserializer)
				.GetMethod(nameof(GetOrCreate), BindingFlags.NonPublic | BindingFlags.Instance);
		}

		public object Deserialize(QueueProperties queueProps, Type type, byte[] data)
		{
			var deserializer = GetOrCreateDynamic(type);
			return AsyncHelper.RunSync(async () => await deserializer.DeserializeAsync(queueProps.QueueName, data));
		}

		public void Dispose()
			=> _registry.Dispose();

		private AvroDeserializerWrapper GetOrCreate<T>()
			=> _deserializers.GetOrAdd(
				typeof(T),
				type => new AvroDeserializerWrapper<T>(_registry)
			);

		private AvroDeserializerWrapper GetOrCreateDynamic(Type type)
		{
			if (!_deserializers.TryGetValue(type, out var deserializer))
			{
				var generic = _getOrCreateMethodInfo?.MakeGenericMethod(type);
				deserializer = (AvroDeserializerWrapper)generic?.Invoke(this, null);
			}

			return deserializer;
		}

		private abstract class AvroDeserializerWrapper
		{
			public abstract Task<object> DeserializeAsync(string topicName, byte[] data);
		}

		private class AvroDeserializerWrapper<T> : AvroDeserializerWrapper
		{
			private readonly AvroDeserializer<T> _deserializer;

			public AvroDeserializerWrapper(ISchemaRegistryClient schemaRegistry)
			{
				_deserializer = new AvroDeserializer<T>(schemaRegistry);
			}

			public override async Task<object> DeserializeAsync(string topicName, byte[] data)
				=> await _deserializer.DeserializeAsync(
					new ReadOnlyMemory<byte>(data),
					false,
					new Confluent.Kafka.SerializationContext(MessageComponentType.Value, topicName)
				);
		}
	}
}
