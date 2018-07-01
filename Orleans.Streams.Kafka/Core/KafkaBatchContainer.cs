using System;
using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaBatchContainer : IBatchContainer
	{
		public Guid StreamGuid { get; }
		public string StreamNamespace { get; }
		public StreamSequenceToken SequenceToken { get; }
		
		public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
		{
			throw new NotImplementedException();
		}

		public bool ImportRequestContext()
		{
			throw new NotImplementedException();
		}

		public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
		{
			throw new NotImplementedException();
		}
	}
}