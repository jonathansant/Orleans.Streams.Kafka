using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streams.Kafka.Core
{
	[Serializable]
	public class KafkaBatchContainer : IBatchContainer, IComparable<KafkaBatchContainer>
	{
		private readonly List<object> _events;
		private readonly Dictionary<string, object> _requestContext;
		private readonly bool _isExternalBatch;
		private readonly IExternalStreamSerializer _serializer;

		public Guid StreamGuid { get; }

		public string StreamNamespace { get; }

		public StreamSequenceToken SequenceToken { get; internal set; }

		[NonSerialized] internal TopicPartitionOffset TopicPartitionOffSet;

		public KafkaBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<object> events,
			Dictionary<string, object> requestContext,
			bool isExternalBatch,
			EventSequenceTokenV2 streamSequenceToken,
			TopicPartitionOffset offset,
			IExternalStreamSerializer serializer
		) : this(streamGuid, streamNamespace, events, requestContext, isExternalBatch)
		{
			SequenceToken = streamSequenceToken;
			TopicPartitionOffSet = offset;
			_serializer = serializer;
		}

		public KafkaBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<object> events,
			Dictionary<string, object> requestContext,
			bool isExternalBatch
		)
		{
			_events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events.");

			StreamGuid = streamGuid;
			StreamNamespace = streamNamespace;

			_requestContext = requestContext;
			_isExternalBatch = isExternalBatch;
		}

		public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
		{
			var sequenceToken = (EventSequenceTokenV2)SequenceToken;

			if (_isExternalBatch)
				return SerializeExternalEvents<T>(sequenceToken);

			return _events
				.OfType<T>()
				.Select((@event, iteration) =>
					Tuple.Create<T, StreamSequenceToken>(
						@event,
						sequenceToken.CreateSequenceTokenForEvent(iteration))
				);
		}

		public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
		{
			// If there is something in this batch that the consumer is interested in, we should send it
			// else the consumer is not interested in any of these events, so don't send.
			return _events.Any(item => shouldReceiveFunc(stream, filterData, item));
		}


		public bool ImportRequestContext()
		{
			if (_requestContext == null)
				return false;

			foreach (var contextProperties in _requestContext)
			{
				RequestContext.Set(contextProperties.Key, contextProperties.Value);
			}

			return true;
		}

		public int CompareTo(KafkaBatchContainer other)
			=> TopicPartitionOffSet.Offset.Value.CompareTo(other.TopicPartitionOffSet.Offset.Value);

		public override string ToString()
			=> $"[KafkaBatchContainer:Stream={StreamGuid},#Items={_events.Count}]";

		private IEnumerable<Tuple<T, StreamSequenceToken>> SerializeExternalEvents<T>(EventSequenceTokenV2 sequenceToken)
		{
			var serializedEvents = _events
				.Select((@event, iteration) =>
				{
					try
					{
						T message;
						var messageType = typeof(T);

						if (messageType.IsPrimitive || messageType == typeof(string) || messageType == typeof(decimal))
							message = (T)Convert.ChangeType(@event, typeof(T));
						else
							message = _serializer.Deserialize<T>(@event);

						return Tuple.Create<T, StreamSequenceToken>(message, sequenceToken.CreateSequenceTokenForEvent(iteration));
					}
					catch (Exception)
					{
						return null;
					}
				})
				.Where(@event => @event != null);

			return serializedEvents;
		}
	}
}