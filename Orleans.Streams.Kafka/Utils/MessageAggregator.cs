//using Confluent.Kafka;
//using Microsoft.Extensions.Logging;
//using Orleans.Runtime;
//using Orleans.Serialization;
//using Orleans.Streams.Kafka.Core;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//
//namespace Orleans.Streams.Kafka.Utils
//{
//	public class MessageAggregator
//	{
//		private readonly Consumer _consumer;
//		private readonly SerializationManager _serializationManager;
//		private readonly ILogger<MessageAggregator> _logger;
//		private readonly Queue<Message> _consumedMessages = new Queue<Message>();
//		private IList<IBatchContainer> _messages;
//
//		public MessageAggregator(
//			Consumer consumer,
//			SerializationManager serializationManager,
//			ILogger<MessageAggregator> logger
//		)
//		{
//			_consumer = consumer;
//			_serializationManager = serializationManager;
//			_logger = logger;
//
//			_consumer.OnMessage += OnMessageReceived;
//		}
//
//		public async Task<IList<IBatchContainer>> Poll(TimeSpan timeout)
//		{
//			_messages = new List<IBatchContainer>();
//			await Task.Run(() => _consumer.Poll(timeout));
//
//			return _messages;
//		}
//
//		public IEnumerable<TopicPartitionOffset> PeekConsumedMessages()
//			=> _consumedMessages.Select(message => new TopicPartitionOffset(message.TopicPartition, message.Offset));
//
//		public void PopConsumedMessages() 
//			=> _consumedMessages.Clear();
//
//		private void OnMessageReceived(object sender, Message message)
//		{
//			var batch = KafkaBatchContainer.ToBatchContainer(message, _serializationManager);
//
//			_logger.Info("Received batch: @batch", batch);
//			_messages.Add(batch);
//			_consumedMessages.Enqueue(message);
//		}
//	}
//}
