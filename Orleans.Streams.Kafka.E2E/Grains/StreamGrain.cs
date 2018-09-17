using System.Threading.Tasks;
using Orleans.Streams.Utils.Streams;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public class StreamGrain : BaseTestGrain, IStreamGrain
	{
		private TestModel _state;

		public Task<string> SaySomething(string something)
		{
			return Task.FromResult(something);
		}

		public Task<TestModel> WhatDidIGet()
		{
			return Task.FromResult(_state);
		}

		public override async Task OnActivateAsync()
		{
			var kafkaProvider = GetStreamProvider(Consts.KafkaStreamProvider);
			var testStream = kafkaProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);
			
			var subscriptionHandles = await testStream.GetAllSubscriptionHandles();
			if (subscriptionHandles.Count > 0)
			{
				foreach (var subscriptionHandle in subscriptionHandles)
				{
					await subscriptionHandle.ResumeAsync(OnNextTestMessage);
				}
			}

			await testStream.SubscribeAsync(OnNextTestMessage);
		}

		private Task OnNextTestMessage(TestModel message, StreamSequenceToken sequenceToken)
		{
			_state = message;

			return Task.CompletedTask;
		}
	}
}
