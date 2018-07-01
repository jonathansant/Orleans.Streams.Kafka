using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Utils;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterFactory : IQueueAdapterFactory
	{
		private readonly KafkaStreamOptions _options;
		private readonly IQueueAdapterCache _adapterCache;
		private readonly IStreamQueueMapper _streamQueueMapper;

		public KafkaAdapterFactory(
			string name,
			KafkaStreamOptions options,
			SimpleQueueCacheOptions cacheOptions,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));
			
			if(!options.InternallyManagedQueuesOnly && options?.Topics.Count == 0)
				throw new ArgumentNullException(nameof(options.Topics));

			_adapterCache = new SimpleQueueAdapterCache(
				cacheOptions, 
				name, 
				loggerFactory
			);

			_streamQueueMapper = _options.InternallyManagedQueuesOnly 
				? new HashRingBasedStreamQueueMapper(new HashRingStreamQueueMapperOptions(), name)
				: (IConsistentRingStreamQueueMapper)new ExternalQueueMapper(GetQueuesProperties());
		}
		
		public Task<IQueueAdapter> CreateAdapter()
		{
			throw new System.NotImplementedException();
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

		private IEnumerable<QueueProperties> GetQueuesProperties()
		{
			// a bit hacky but confluent doesn't seem to have a management API
			using (var producer = new Producer(_options.ToProducerProperties(), true, true))
			{
				return from kafkaTopic in producer.GetMetadata().Topics
					join userTopic in _options.Topics on kafkaTopic.Topic equals userTopic
					from partition in kafkaTopic.Partitions
					select new QueueProperties(userTopic, (uint)partition.PartitionId);
			}
		}
	}
}