namespace Orleans.Streams.Kafka.Utils
{
	public static class QueueIdExtensions
	{
		public static string GetQueueNamespace(this QueueId queueId)
		{
			var prefix = queueId.GetStringNamePrefix();
			return prefix.Substring(0, prefix.IndexOf('_'));
		}
	}
}
