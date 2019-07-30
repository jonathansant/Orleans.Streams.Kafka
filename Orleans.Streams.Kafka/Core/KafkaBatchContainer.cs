using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams.Utils.MessageTracking;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streams.Kafka.Core
{
	[Serializable]
	public class KafkaBatchContainer : ITraceablebleBatch, IComparable<KafkaBatchContainer>
	{
		private readonly Dictionary<string, object> _requestContext;

		[NonSerialized] internal TopicPartitionOffset TopicPartitionOffSet;

		protected List<object> Events { get; set; }

		public Guid StreamGuid { get; }

		public string StreamNamespace { get; }

		public StreamSequenceToken SequenceToken { get; internal set; }

		public List<object> RawEvents => Events;

		public KafkaBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<object> events,
			Dictionary<string, object> requestContext
		) : this(streamGuid, streamNamespace, events, requestContext, null, null)
		{
		}

		public KafkaBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<object> events,
			Dictionary<string, object> requestContext,
			EventSequenceTokenV2 streamSequenceToken,
			TopicPartitionOffset offset
		)
		{
			Events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events.");

			StreamGuid = streamGuid;
			StreamNamespace = streamNamespace;
			SequenceToken = streamSequenceToken;
			TopicPartitionOffSet = offset;
			_requestContext = requestContext;
		}

		public virtual IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
		{
			var sequenceToken = (EventSequenceTokenV2)SequenceToken;
			return Events
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
			return Events.Any(item => shouldReceiveFunc(stream, filterData, item));
		}


		public bool ImportRequestContext()
		{
			if (_requestContext == null)
				return false;

			foreach (var contextProperties in _requestContext)
				RequestContext.Set(contextProperties.Key, contextProperties.Value);

			return true;
		}

		public int CompareTo(KafkaBatchContainer other)
			=> TopicPartitionOffSet.Offset.Value.CompareTo(other.TopicPartitionOffSet.Offset.Value);

		public override string ToString()
			=> $"[{GetType().Name}:Stream={StreamGuid},#Items={Events.Count}]";
	}
}