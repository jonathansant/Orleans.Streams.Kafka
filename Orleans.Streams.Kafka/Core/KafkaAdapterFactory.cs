using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterFactory : IQueueAdapterFactory
	{
		private readonly string _name;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly ILoggerFactory _loggerFactory;
		private readonly IGrainFactory _grainFactory;
		private readonly IExternalStreamDeserializer _externalDeserializer;
		private readonly IQueueAdapterCache _adapterCache;
		private readonly IStreamQueueMapper _streamQueueMapper;
		private readonly ILogger<KafkaAdapterFactory> _logger;
		private readonly IDictionary<string, QueueProperties> _queueProperties;

		public KafkaAdapterFactory(
			string name,
			KafkaStreamOptions options,
			SimpleQueueCacheOptions cacheOptions,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory
		) : this(name, options, cacheOptions, serializationManager, loggerFactory, grainFactory, null)
		{
			if (options.Topics.Any(topic => topic.IsExternal))
				throw new InvalidOperationException(
				"Cannot have external topic with no 'IExternalDeserializer' defined. Use 'AddJson' or 'AddAvro'"
			);
		}

		public KafkaAdapterFactory(
			string name,
			KafkaStreamOptions options,
			SimpleQueueCacheOptions cacheOptions,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory,
			IExternalStreamDeserializer externalDeserializer
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_name = name;
			_serializationManager = serializationManager;
			_loggerFactory = loggerFactory;
			_grainFactory = grainFactory;
			_externalDeserializer = externalDeserializer;
			_logger = loggerFactory.CreateLogger<KafkaAdapterFactory>();

			if (options.Topics != null && options.Topics.Count == 0)
				throw new ArgumentNullException(nameof(options.Topics));

			_adapterCache = new SimpleQueueAdapterCache(
				cacheOptions,
				name,
				loggerFactory
			);

			_queueProperties = GetQueuesProperties();
			_streamQueueMapper = new ExternalQueueMapper(_queueProperties.Values);
		}

		public Task<IQueueAdapter> CreateAdapter()
		{
			var adapter = new KafkaAdapter(
				_name,
				_options,
				_queueProperties,
				_serializationManager,
				_loggerFactory,
				_grainFactory,
				_externalDeserializer
			);

			return Task.FromResult<IQueueAdapter>(adapter);
		}

		public IQueueAdapterCache GetQueueAdapterCache()
			=> _adapterCache;

		public IStreamQueueMapper GetStreamQueueMapper()
			=> _streamQueueMapper;

		public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
			=> Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));

		public static KafkaAdapterFactory Create(IServiceProvider services, string name)
		{
			var streamsConfig = services.GetOptionsByName<KafkaStreamOptions>(name);
			var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
			var deserializer = services.GetServiceByName<IExternalStreamDeserializer>(name);

			KafkaAdapterFactory factory;
			if (deserializer != null)
				factory = ActivatorUtilities.CreateInstance<KafkaAdapterFactory>(
					services,
					name,
					streamsConfig,
					cacheOptions,
					deserializer
				);
			else
				factory = ActivatorUtilities.CreateInstance<KafkaAdapterFactory>(
					services,
					name,
					streamsConfig,
					cacheOptions
				);

			return factory;
		}

		private IDictionary<string, QueueProperties> GetQueuesProperties()
		{
			var config = _options.ToAdminProperties();

			try
			{
				using (var admin = new AdminClientBuilder(config).Build())
				{
					var meta = admin.GetMetadata(_options.AdminRequestTimeout);
					var props = from kafkaTopic in meta.Topics
								join userTopic in _options.Topics on kafkaTopic.Topic equals userTopic.Name
								from partition in kafkaTopic.Partitions
								select new QueueProperties(
									userTopic.Name,
									(uint)partition.PartitionId,
									userTopic.IsExternal
								);

					return props.ToDictionary(prop => prop.QueueName);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to retrieve Kafka meta data. {@config}", config);
				throw;
			}
		}
	}
}