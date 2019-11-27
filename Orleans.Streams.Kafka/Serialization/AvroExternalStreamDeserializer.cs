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
	public class AvroExternalStreamSerializer : IExternalStreamSerDes
	{
		private readonly ConcurrentDictionary<Type, AvroSerializerWrapper> _deserializers;
		private readonly ISchemaRegistryClient _registry;
		private readonly MethodInfo _getOrCreateMethodInfo;

		public AvroExternalStreamSerializer(ISchemaRegistryClient schemaRegistry)
		{
			_registry = schemaRegistry;
			_deserializers = new ConcurrentDictionary<Type, AvroSerializerWrapper>();
			_getOrCreateMethodInfo = typeof(AvroExternalStreamSerializer)
				.GetMethod(nameof(GetOrCreate), BindingFlags.NonPublic | BindingFlags.Instance);
		}

		public object Deserialize(QueueProperties queueProps, Type type, byte[] data)
		{
			var serializer = GetOrCreateDynamic(type);
			return AsyncHelper.RunSync(async () => await serializer.DeserializeAsync(queueProps.QueueName, data));
		}

		public byte[] Serialize(QueueProperties queueProps, Type type, object data)
		{
			var serializer = GetOrCreateDynamic(type);
			return AsyncHelper.RunSync(async () => await serializer.SerializeAsync(queueProps.QueueName, data));
		}

		public void Dispose()
			=> _registry.Dispose();

		private AvroSerializerWrapper GetOrCreate<T>()
			=> _deserializers.GetOrAdd(
				typeof(T),
				type => new AvroSerializerWrapper<T>(_registry)
			);

		private AvroSerializerWrapper GetOrCreateDynamic(Type type)
		{
			if (_deserializers.TryGetValue(type, out var deserializer))
				return deserializer;

			var generic = _getOrCreateMethodInfo?.MakeGenericMethod(type);
			deserializer = (AvroSerializerWrapper)generic?.Invoke(this, null);

			return deserializer;
		}

		private abstract class AvroSerializerWrapper
		{
			public abstract Task<object> DeserializeAsync(string topicName, byte[] data);
			public abstract Task<byte[]> SerializeAsync(string topicName, object data);
		}

		private class AvroSerializerWrapper<T> : AvroSerializerWrapper
		{
			private readonly AvroDeserializer<T> _deserializer;
			private readonly AvroSerializer<T> _serializer;

			public AvroSerializerWrapper(ISchemaRegistryClient schemaRegistry)
			{
				_deserializer = new AvroDeserializer<T>(schemaRegistry);
				_serializer = new AvroSerializer<T>(schemaRegistry);
			}

			public override async Task<object> DeserializeAsync(string topicName, byte[] data)
				=> await _deserializer.DeserializeAsync(
					new ReadOnlyMemory<byte>(data),
					false,
					new Confluent.Kafka.SerializationContext(MessageComponentType.Value, topicName)
				);

			public override Task<byte[]> SerializeAsync(string topicName, object data)
				=> SerializeAsync(topicName, (T)data);

			private async Task<byte[]> SerializeAsync(string topicName, T data)
				=> await _serializer.SerializeAsync(
					data,
					new Confluent.Kafka.SerializationContext(MessageComponentType.Value, topicName)
				);
		}
	}
}
