using Confluent.Kafka;
using Newtonsoft.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streams.Kafka.Core
{
	[Serializable]
	public class KafkaBatchContainer : IBatchContainer
	{
		private readonly List<object> _events;
		private readonly Dictionary<string, object> _requestContext;
		private readonly bool _isExternalBatch;

		public Guid StreamGuid { get; }
		public string StreamNamespace { get; }
		public StreamSequenceToken SequenceToken { get; internal set; }

		public KafkaBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<object> events,
			Dictionary<string, object> requestContext,
			bool isExternalBatch,
			EventSequenceTokenV2 streamSequenceToken
		) : this(streamGuid, streamNamespace, events, requestContext, isExternalBatch)
		{
			SequenceToken = streamSequenceToken;
		}

		public KafkaBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<object> events,
			Dictionary<string, object> requestContext,
			bool isExternalBatch
		)
		{
			_events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");

			StreamGuid = streamGuid;
			StreamNamespace = streamNamespace;

			_requestContext = requestContext;
			_isExternalBatch = isExternalBatch;
		}

		internal static IBatchContainer ToBatchContainer(Message message, SerializationManager serializationManager)
			=> serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(message.Value);

		public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
		{
			var sequenceToken = (EventSequenceTokenV2)SequenceToken;

			if (!_isExternalBatch)
			{
				return _events
					.OfType<T>()
					.Select((@event, iteration) =>
						Tuple.Create<T, StreamSequenceToken>(
							@event,
							sequenceToken.CreateSequenceTokenForEvent(iteration))
						);
			}

			return _events
				.Select((@event, iteration) =>
				{
					try
					{
						T message;

						if (typeof(T).IsPrimitive || typeof(T) == typeof(string) || typeof(T) == typeof(decimal))
						{
							message = (T)Convert.ChangeType(@event, typeof(T));
						}
						else
						{
							message = JsonConvert.DeserializeObject<T>((string)@event); // todo: support for multiple serializer
						}

						return Tuple.Create<T, StreamSequenceToken>(message, sequenceToken.CreateSequenceTokenForEvent(iteration));
					}
					catch (Exception)
					{
						return null;
					}
				})
				.Where(@event => @event != null);
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

		public byte[] ToByteArray(SerializationManager serializationManager)
			=> serializationManager.SerializeToByteArray(this);

		public override string ToString()
			=> $"[KafkaBatchContainer:Stream={StreamGuid},#Items={_events.Count}]";
	}
}