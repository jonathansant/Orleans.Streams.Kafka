using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.E2E.Extensions;
using Orleans.Streams.Kafka.E2E.Grains;
using Orleans.Streams.Kafka.E2E.Serialization;
using Orleans.TestingHost;
using System;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace Orleans.Streams.Kafka.E2E.Tests
{
	public class ClusteredStreamTests : TestBase
	{
		private const int ReceiveDelay = 500;

		public ClusteredStreamTests()
		{
			Initialize(3); // Initialize with three silos so that queues will be load-balanced on different silos.
		}

		[Fact]
		public async Task ProduceConsumeTest()
		{
			var grain = await WakeUpGrain<IRoundTripGrain>();
			var result = await grain.Fire();

			Assert.Equal(result.Expected, result.Actual);
		}

		[Fact]
		public async Task ConsumeInOrderMultipleStreams()
		{
			var grain = await WakeUpGrain<IMultiStreamGrain>();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);
			var stream2 = streamProvider.GetStream<TestModel>(Consts.StreamId2, Consts.StreamNamespace2);

			var result = grain.Fire();

			TestModel lastMessage = null;
			TestModel lastMessage2 = null;
			for (var i = 0; i < 10; i++)
			{
				var testMessage = TestModel.Random();
				var testMessage2 = TestModel.Random();

				if (i == 9)
				{
					testMessage.IsLastMessage = true;
					testMessage2.IsLastMessage = true;
				}

				await Task.WhenAll(stream.OnNextAsync(testMessage), stream2.OnNextAsync(testMessage2));
				lastMessage = testMessage;
				lastMessage2 = testMessage2;
			}

			await Task.WhenAny(result, Task.Delay(ReceiveDelay * 4));

			if (!result.IsCompleted)
				throw new XunitException("Message not received.");

			Assert.Equal(lastMessage, result.Result.StreamResult1.Actual);
			Assert.Equal(lastMessage2, result.Result.StreamResult2.Actual);
		}

		private async Task<TGrain> WakeUpGrain<TGrain>() where TGrain : IBaseTestGrain
		{
			var grain = Cluster.GrainFactory.GetGrain<TGrain>("testGrain");
			await grain.WakeUp();

			return grain;
		}
	}

	public class ClusteredStreamTests_ProduceConsumeExternalMessage : TestBase
	{
		public ClusteredStreamTests_ProduceConsumeExternalMessage()
		{
			Initialize(3);
		}

		[Fact]
		public async Task E2E()
		{
			var config = GetKafkaServerConfig();

			var testMessage = TestModel.Random();

			var completion = new TaskCompletionSource<bool>();

			var provider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = provider.GetStream<TestModel>(Consts.StreamId2, Consts.StreamNamespaceExternal);

			await stream.QuickSubscribe((message, seq) =>
			{
				Assert.Equal(testMessage, message);
				completion.SetResult(true);
				return Task.CompletedTask;
			});

			await Task.Delay(5000);

			using (var producer = new ProducerBuilder<byte[], TestModel>(config)
				.SetValueSerializer(new LowercaseJsonSerializer<TestModel>())
				.Build()
			)
			{
				await producer.ProduceAsync(Consts.StreamNamespaceExternal, new Message<byte[], TestModel>
				{
					Key = Encoding.UTF8.GetBytes(Consts.StreamId2),
					Value = testMessage,
					Timestamp = new Timestamp(DateTimeOffset.UtcNow)
				});
			}

			await Task.WhenAny(completion.Task, Task.Delay(500 * 4));

			if (!completion.Task.IsCompleted)
				throw new XunitException("Message not received.");
		}


		private static ClientConfig GetKafkaServerConfig()
			=> new ClientConfig
			{
				BootstrapServers = string.Join(',', Brokers)
			};
	}

	public class ClusteredStreamTests_GuidStreamId : TestBase
	{
		private const int ReceiveDelay = 500;

		public ClusteredStreamTests_GuidStreamId()
		{
			Initialize(3);
		}

		[Fact]
		public async Task E2E()
		{
			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var newId = Guid.Parse("1bf42d0a-0145-4ff6-9a5c-774559dca2a9");
			var stream = streamProvider.GetStream<TestModel>(newId);

			var expected = TestModel.Random();
			var roundTrip = new TaskCompletionSource<TestModel>();

			await stream.QuickSubscribe((message, seq) =>
			{
				roundTrip.SetResult(message);
				return Task.CompletedTask;
			});

			await Task.Delay(5000);

			await stream.OnNextAsync(expected);

			await Task.WhenAny(Task.Delay(ReceiveDelay * 4), roundTrip.Task);

			if (!roundTrip.Task.IsCompleted)
				throw new XunitException("Message not received.");

			Assert.Equal(expected, roundTrip.Task.Result);
		}
	}

	public class ClusteredStreamTests_AutoTopicCreation : TestBase
	{
		private const int ReceiveDelay = 500;

		public ClusteredStreamTests_AutoTopicCreation()
		{
			Initialize<ClientBuilderConfigurator, SiloBuilderConfigurator>(3);
		}

		public override async Task InitializeAsync()
		{
			using var admin = new AdminClientBuilder(new ClientConfig { BootstrapServers = string.Join(',', Brokers) }).Build();

			try
			{
				await admin.DeleteTopicsAsync(new[] { Consts.StreamNamespaceAuto });
			}
			catch (DeleteTopicsException)
			{
				// ignored
			}

			await base.InitializeAsync();
		}

		[Fact]
		public async Task E2E()
		{
			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var newId = Guid.Parse("1bf42d0a-0145-4ff6-9a5c-774559dca2a9");
			var stream = streamProvider.GetStream<TestModel>(newId);

			var expected = TestModel.Random();
			var roundTrip = new TaskCompletionSource<TestModel>();

			await stream.QuickSubscribe((message, seq) =>
			{
				roundTrip.SetResult(message);
				return Task.CompletedTask;
			});

			await Task.Delay(5000);

			await stream.OnNextAsync(expected);

			await Task.WhenAny(Task.Delay(ReceiveDelay * 4), roundTrip.Task);

			if (!roundTrip.Task.IsCompleted)
				throw new XunitException("Message not received.");

			Assert.Equal(expected, roundTrip.Task.Result);
		}

		public class ClientBuilderConfigurator : IClientBuilderConfigurator
		{
			public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
				=> clientBuilder
					.AddKafka(Consts.KafkaStreamProvider)
					.WithOptions(options =>
					{
						options.BrokerList = TestBase.Brokers;
						options.ConsumerGroupId = "E2EGroup";

						options
							.AddTopic(Consts.StreamNamespace)
							.AddTopic(Consts.StreamNamespaceAuto, new TopicCreationConfig { AutoCreate = true })
							;

						options.PollTimeout = TimeSpan.FromMilliseconds(10);
						options.ConsumeMode = ConsumeMode.StreamEnd;
					})
					.Build()
				;
		}

		public class SiloBuilderConfigurator : ISiloConfigurator
		{
			public void Configure(ISiloBuilder hostBuilder)
				=> hostBuilder
					.AddMemoryGrainStorage("PubSubStore")
					.AddKafka(Consts.KafkaStreamProvider)
					.WithOptions(options =>
					{
						options.BrokerList = TestBase.Brokers;
						options.ConsumerGroupId = "E2EGroup";
						options.ConsumeMode = ConsumeMode.StreamEnd;
						options.PollTimeout = TimeSpan.FromMilliseconds(10);
						options.MessageTrackingEnabled = true;

						options
							.AddTopic(Consts.StreamNamespace)
							.AddTopic(Consts.StreamNamespaceAuto, new TopicCreationConfig { AutoCreate = true })
							;
					})
					.AddLoggingTracker()
					.Build();
		}
	}

	public class ClusteredStreamTests_AutoTopicCreation_TopicExists : TestBase
	{
		private const int ReceiveDelay = 500;

		public ClusteredStreamTests_AutoTopicCreation_TopicExists()
		{
			Initialize<ClientBuilderConfigurator, SiloBuilderConfigurator>(3);
		}

		public override async Task InitializeAsync()
		{
			using var admin = new AdminClientBuilder(new ClientConfig { BootstrapServers = string.Join(',', Brokers) }).Build();

			try
			{
				await admin.DeleteTopicsAsync(new[] { Consts.StreamNamespaceAuto2 });
			}
			catch (DeleteTopicsException)
			{
				// ignored
			}

			await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = Consts.StreamNamespaceAuto2, NumPartitions = 3, ReplicationFactor = 1 } });

			await base.InitializeAsync();
		}

		[Fact]
		public async Task E2E()
		{
			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var newId = Guid.Parse("1bf42d0a-0145-4ff6-9a5c-774559dca2a9");
			var stream = streamProvider.GetStream<TestModel>(newId);

			var expected = TestModel.Random();
			var roundTrip = new TaskCompletionSource<TestModel>();

			await stream.QuickSubscribe((message, seq) =>
			{
				roundTrip.SetResult(message);
				return Task.CompletedTask;
			});

			await Task.Delay(5000);

			await stream.OnNextAsync(expected);

			await Task.WhenAny(Task.Delay(ReceiveDelay * 4), roundTrip.Task);

			if (!roundTrip.Task.IsCompleted)
				throw new XunitException("Message not received.");

			Assert.Equal(expected, roundTrip.Task.Result);
		}

		public class ClientBuilderConfigurator : IClientBuilderConfigurator
		{
			public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
				=> clientBuilder
					.AddKafka(Consts.KafkaStreamProvider)
					.WithOptions(options =>
					{
						options.BrokerList = TestBase.Brokers;
						options.ConsumerGroupId = "E2EGroup";

						options
							.AddTopic(Consts.StreamNamespace)
							.AddTopic(Consts.StreamNamespaceAuto2, new TopicCreationConfig { AutoCreate = true })
							;

						options.PollTimeout = TimeSpan.FromMilliseconds(10);
						options.ConsumeMode = ConsumeMode.StreamEnd;
					})
					.Build()
				;
		}

		public class SiloBuilderConfigurator : ISiloConfigurator
		{
			public void Configure(ISiloBuilder hostBuilder)
				=> hostBuilder
					.AddMemoryGrainStorage("PubSubStore")
					.AddKafka(Consts.KafkaStreamProvider)
					.WithOptions(options =>
					{
						options.BrokerList = TestBase.Brokers;
						options.ConsumerGroupId = "E2EGroup";
						options.ConsumeMode = ConsumeMode.StreamEnd;
						options.PollTimeout = TimeSpan.FromMilliseconds(10);
						options.MessageTrackingEnabled = true;

						options
							.AddTopic(Consts.StreamNamespace)
							.AddTopic(Consts.StreamNamespaceAuto2, new TopicCreationConfig { AutoCreate = true })
							;
					})
					.AddLoggingTracker()
					.Build();
		}
	}

	public class ClusteredStreamTests_DynamicData : TestBase
	{
		private const int ReceiveDelay = 500;

		public ClusteredStreamTests_DynamicData()
		{
			Initialize(3);
		}

		[Fact]
		public async Task E2E()
		{
			var grain = await WakeUpGrain<IRoundTripDynamicModelGrain>();
			var result = await grain.Fire();
			Assert.Equal(result.Expected, result.Actual);
		}

		private async Task<TGrain> WakeUpGrain<TGrain>() where TGrain : IBaseTestGrain
		{
			var grain = Cluster.GrainFactory.GetGrain<TGrain>("testGrain");
			await grain.WakeUp();

			return grain;
		}
	}
}