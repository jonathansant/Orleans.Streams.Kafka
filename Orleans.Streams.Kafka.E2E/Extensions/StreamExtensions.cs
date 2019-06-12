using System;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.E2E.Extensions
{
	public static class StreamExtensions
	{
		public static async Task QuickSubscribe<T>(this IAsyncStream<T> stream, Func<T, StreamSequenceToken, Task> onNextAsync)
		{
			var subscriptionHandles = await stream.GetAllSubscriptionHandles();
			if (subscriptionHandles.Count > 0)
				foreach (var subscriptionHandle in subscriptionHandles)
					await subscriptionHandle.ResumeAsync(onNextAsync);

			await stream.SubscribeAsync(onNextAsync);
		}
	}
}
