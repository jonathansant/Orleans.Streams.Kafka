using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Streams.Utils.Streams;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterFactory : IQueueAdapterFactory
	{
		private readonly string _name;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly ILoggerFactory _loggerFactory;
		private readonly IQueueAdapterCache _adapterCache;
		private readonly IStreamQueueMapper _streamQueueMapper;
		private readonly ILogger<KafkaAdapterFactory> _logger;
		private readonly IDictionary<string, QueueProperties> _queueProperties;

		public KafkaAdapterFactory(
			string name,
			KafkaStreamOptions options,
			SimpleQueueCacheOptions cacheOptions,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_name = name;
			_serializationManager = serializationManager;
			_loggerFactory = loggerFactory;
			_logger = loggerFactory.CreateLogger<KafkaAdapterFactory>();

			if (!options.InternallyManagedQueuesOnly && options.Topics != null && options.Topics.Count == 0)
				throw new ArgumentNullException(nameof(options.Topics));

			_adapterCache = new SimpleQueueAdapterCache(
				cacheOptions,
				name,
				loggerFactory
			);

			_queueProperties = GetQueuesProperties();

			_streamQueueMapper = _options.InternallyManagedQueuesOnly
				? new HashRingBasedStreamQueueMapper(new HashRingStreamQueueMapperOptions(), name)
				: (IConsistentRingStreamQueueMapper)new ExternalQueueMapper(_queueProperties.Values);
		}

		public Task<IQueueAdapter> CreateAdapter()
		{
			var adapter = new KafkaAdapter(
				_name,
				_options,
				_queueProperties,
				_serializationManager,
				_loggerFactory
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

			var factory = ActivatorUtilities.CreateInstance<KafkaAdapterFactory>(
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
				using (var admin = new AdminClient(config))
				{
					var meta = admin.GetMetadata(_options.AdminRequestTimeout); // todo: add new option
					var props = from kafkaTopic in meta.Topics
						join userTopic in _options.Topics on kafkaTopic.Topic equals userTopic
						from partition in kafkaTopic.Partitions
						select new QueueProperties(userTopic, (uint) partition.PartitionId);

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