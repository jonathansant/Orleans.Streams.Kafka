using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Consumer;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SerializationContext = Orleans.Streams.Kafka.Serialization.SerializationContext;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterReceiver : IQueueAdapterReceiver
	{
		private readonly ILogger<KafkaAdapterReceiver> _logger;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly IGrainFactory _grainFactory;
		private readonly IExternalStreamDeserializer _externalDeserializer;
		private readonly QueueProperties _queueProperties;

		private IConsumer<byte[], byte[]> _consumer;
		private Task _commitPromise = Task.CompletedTask;
		private Task<IList<IBatchContainer>> _consumePromise;

		public KafkaAdapterReceiver(
			QueueProperties queueProperties,
			KafkaStreamOptions options,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory,
			IExternalStreamDeserializer externalDeserializer
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_queueProperties = queueProperties;
			_serializationManager = serializationManager;
			_grainFactory = grainFactory;
			_externalDeserializer = externalDeserializer;
			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();
		}

		public Task Initialize(TimeSpan timeout)
		{

			_consumer = new ConsumerBuilder<byte[], byte[]>(_options.ToConsumerProperties())
				.SetErrorHandler((sender, errorEvent) =>
					_logger.LogError(
						"Consume error reason: {reason}, code: {code}, is broker error: {errorType}",
						errorEvent.Reason,
						errorEvent.Code,
						errorEvent.IsBrokerError
					))
				.Build();

			var offsetMode = Offset.Stored;
			switch (_options.ConsumeMode)
			{
				case ConsumeMode.LastCommittedMessage:
					offsetMode = Offset.Stored;
					break;
				case ConsumeMode.StreamEnd:
					offsetMode = Offset.End;
					break;
				case ConsumeMode.StreamStart:
					offsetMode = Offset.Beginning;
					break;
			}

			_consumer.Assign(new TopicPartitionOffset(_queueProperties.Namespace, (int)_queueProperties.PartitionId, offsetMode));

			return Task.CompletedTask;
		}

		public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.

			if (consumerRef == null)
				return Task.FromResult<IList<IBatchContainer>>(new List<IBatchContainer>());

			var cancellationSource = new CancellationTokenSource();
			cancellationSource.CancelAfter(_options.PollBufferTimeout);

			_consumePromise = Task.Run(
				() => PollForMessages(
					maxCount,
					cancellationSource
				),
				cancellationSource.Token
			);

			return _consumePromise;
		}

		public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			KafkaBatchContainer batchWithHighestOffset = null;

			try
			{
				if (!messages.Any())
					return;

				batchWithHighestOffset = messages
					.Cast<KafkaBatchContainer>()
					.Max();

				_commitPromise = _consumer.Commit(batchWithHighestOffset);
				await _commitPromise;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to commit message offset: {@offset}", batchWithHighestOffset?.TopicPartitionOffSet);
				throw;
			}
		}

		public async Task Shutdown(TimeSpan timeout)
		{
			try
			{
				await Task.WhenAll(_commitPromise, _consumePromise);
			}
			finally
			{
				_consumer.Unassign();
				_consumer.Unsubscribe();
				_consumer.Close();
				_consumer = null;
			}
		}

		private async Task<IList<IBatchContainer>> PollForMessages(int maxCount, CancellationTokenSource cancellation)
		{
			try
			{
				var batches = new List<IBatchContainer>();
				for (var i = 0; i < maxCount && !cancellation.IsCancellationRequested; i++)
				{
					var consumeResult = _consumer.Consume(_options.PollTimeout);
					if (consumeResult == null)
						break;


					var batchContainer = consumeResult.ToBatchContainer(
						new SerializationContext
						{
							SerializationManager = _serializationManager,
							ExternalStreamDeserializer = _externalDeserializer
						},
						_queueProperties
					);

					await TrackMessage(batchContainer);

					batches.Add(batchContainer);
				}

				return batches;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to poll for messages queueId: {@queueProperties}", _queueProperties);
				throw;
			}
			finally
			{
				cancellation.Dispose();
			}
		}

		private Task TrackMessage(IBatchContainer container)
		{
			if (!_options.MessageTrackingEnabled)
				return Task.CompletedTask;

			var trackingGrain = _grainFactory.GetMessageTrackerGrain(_queueProperties.QueueName);
			return trackingGrain.Track(container);
		}
	}
}