using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Utils;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterReceiver : IQueueAdapterReceiver
	{
		private readonly ILogger<KafkaAdapterReceiver> _logger;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly QueueId _queueId;
		private readonly List<ConsumeResult<byte[], byte[]>> _consumedMessages;
		private readonly string _streamNamespace;

		private Consumer<byte[], byte[]> _consumer;
		private Task _outstandingPromise;

		public KafkaAdapterReceiver(
			QueueId queueId,
			KafkaStreamOptions options,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory
		)
		{
			_queueId = queueId ?? throw new ArgumentNullException(nameof(queueId));
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_serializationManager = serializationManager;
			_consumedMessages = new List<ConsumeResult<byte[], byte[]>>();

			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();

			_streamNamespace = queueId.GetQueueNamespace();
		}

		public Task Initialize(TimeSpan timeout)
		{
			_consumer = new Consumer<byte[], byte[]>(
				_options.ToConsumerProperties(),
				new ByteArrayDeserializer(),
				new ByteArrayDeserializer()
			);

			_consumer.Assign(new TopicPartitionOffset(_streamNamespace, (int)_queueId.GetNumericId(), Offset.Stored));

			return Task.CompletedTask;
		}

		public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.

			if (consumerRef == null)
				return new List<IBatchContainer>();

			try
			{
				var messagePromise = _consumer.Poll(TimeSpan.FromMilliseconds(_options.PollTimeout));

				_outstandingPromise = messagePromise;

				var consumeResult = await messagePromise;
				if (consumeResult != null)
				{
					_logger.Info("Received batch: {@batch}", consumeResult.Value);

					var batchContainer = consumeResult.ToBatchContainer(_serializationManager, _streamNamespace);
					_consumedMessages.Add(consumeResult);

					return new List<IBatchContainer> { batchContainer };
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to poll for messages.");
				throw;
			}

			return new List<IBatchContainer>();
		}

		public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			try
			{
				await _consumer.Commit(_consumedMessages);
				_consumedMessages.Clear();
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to commit messages. {@messages}", messages);
				throw;// todo: should we throw?
			}
		}

		public async Task Shutdown(TimeSpan timeout)
		{
			try
			{
				if (_outstandingPromise != null)
					await _outstandingPromise;
			}
			finally
			{
				_consumer.Dispose();
				_consumer = null;
			}
		}
	}
}