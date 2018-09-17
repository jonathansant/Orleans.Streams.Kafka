using Orleans.Streams.Kafka.E2E.Extensions;
using Orleans.Streams.Kafka.E2E.Grains;
using Orleans.Streams.Utils.Streams;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace Orleans.Streams.Kafka.E2E.Tests
{
	public class SingleClusterStreamTests : TestBase
	{
		public SingleClusterStreamTests()
		{
			Initialize(1);
		}

		[Fact]
		public async Task BasicProduceConsumeTest()
		{
			var grain = Cluster.GrainFactory.GetGrain<IStreamGrain>("testGrain");
			await grain.WakeUp();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);

			var testMessage = TestModel.Random();

			await stream.OnNextAsync(testMessage);

			await Task.Delay(500);
			var response = await grain.WhatDidIGet();

			Assert.Equal(testMessage, response);
		}


		[Fact]
		public async Task BasicRoundTripTest()
		{
			var grain = Cluster.GrainFactory.GetGrain<IStreamRoundTripGrain>("testGrain");
			await grain.WakeUp();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId2, Consts.StreamNamespace);

			var testMessage = TestModel.Random();

			var completion = new TaskCompletionSource<bool>();

			await streamProvider
				.GetStream<TestModel>(Consts.ResponseStreamId, Consts.StreamNamespace)
				.QuickSubscribe((message, seq) =>
				{
					Assert.Equal(testMessage, message);
					completion.SetResult(true);
					return Task.CompletedTask;
				});

			await stream.OnNextAsync(testMessage);

			await Task.WhenAny(completion.Task, Task.Delay(10000));

			if (!completion.Task.IsCompleted)
				throw new XunitException("Did not receive Response Message.");
		}
	}
}
