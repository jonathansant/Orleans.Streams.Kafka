using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Utils;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Core
{
	using System.Globalization;

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
		private readonly AdminClientBuilder _adminConfig;
		private readonly AdminClientConfig _config;

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
			_adminConfig = new AdminClientBuilder(options.ToAdminProperties());

			if (options.Topics != null && options.Topics.Count == 0)
				throw new ArgumentNullException(nameof(options.Topics));

			_adapterCache = new SimpleQueueAdapterCache(
				cacheOptions,
				name,
				loggerFactory
			);

			_queueProperties = GetQueuesProperties().ToDictionary(q => q.QueueName);
			_streamQueueMapper = new ExternalQueueMapper(_queueProperties.Values);

			_config = _options.ToAdminProperties();
		}

		public Task<IQueueAdapter> CreateAdapter()
			=> Task.FromResult((IQueueAdapter)new KafkaAdapter(
				_name,
				_options,
				_queueProperties,
				_serializationManager,
				_loggerFactory,
				_grainFactory,
				_externalDeserializer
			));

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

		private IEnumerable<QueueProperties> GetQueuesProperties()
		{
			try
			{
				using var admin = _adminConfig.Build();
				var meta = admin.GetMetadata(_options.AdminRequestTimeout);
				var currentMetaTopics = meta.Topics.ToList();

				var props = new List<QueueProperties>();
				var autoProps = new List<(QueueProperties props, short replicationFactor, ulong retentionPeriodInMs )>();

				foreach (var topic in _options.Topics)
				{
					if (!topic.AutoCreate || meta.Topics.Any(kt => kt.Topic == topic.Name))
						continue;

					var noOfPartitions = topic.Partitions == -1 ? 1 : topic.Partitions;
					for (var i = 0; i < noOfPartitions; i++)
					{
						var prop = CreateQueueProperty(topic, partitionId: i);
						props.Add(prop);
						autoProps.Add((prop, topic.ReplicationFactor, topic.RetentionPeriodInMs));
					}
				}

				AsyncHelper.RunSync(() => CreateAutoTopics(admin, autoProps));

				props.AddRange(
					from kafkaTopic in currentMetaTopics
					join userTopic in _options.Topics on kafkaTopic.Topic equals userTopic.Name
					from partition in kafkaTopic.Partitions
					select CreateQueueProperty(userTopic, partition)
				);

				return props;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to retrieve Kafka meta data. {@config}", _config);
				throw;
			}

			static QueueProperties CreateQueueProperty(
				TopicConfig userTopic,
				PartitionMetadata partition = null,
				int partitionId = -1
			) => new QueueProperties(
					userTopic.Name,
					(uint)(partition?.PartitionId ?? partitionId),
					userTopic.IsExternal,
					userTopic.ExternalContractType
				);
		}

		private static Task CreateAutoTopics(IAdminClient admin, IEnumerable<(QueueProperties prop, short replicationFactor, ulong retentionPeriodInMs)> autoQueues)
		{
			var topics = autoQueues
					.GroupBy(queue => queue.prop.Namespace)
					.Aggregate(
						new List<TopicSpecification>(),
						(result, queues) =>
						{
							var tuple = queues.First();

							result.Add(new TopicSpecification
							{
								Name = queues.Key,
								NumPartitions = queues.Count(),
								ReplicationFactor = tuple.replicationFactor,
								Configs = new Dictionary<string, string>()
								          {
									          {"retention.ms", tuple.retentionPeriodInMs.ToString(CultureInfo.InvariantCulture) }
								          }
							});

							return result;
						}
					)
				;

			return topics.Any()
				? admin.CreateTopicsAsync(topics)
				: Task.CompletedTask;
		}
	}
}