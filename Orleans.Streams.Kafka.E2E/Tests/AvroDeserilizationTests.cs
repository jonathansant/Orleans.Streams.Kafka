using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.E2E.Extensions;
using Orleans.Streams.Kafka.E2E.Grains;
using Orleans.TestingHost;
using System;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace Orleans.Streams.Kafka.E2E.Tests
{
	public class AvroDeserilizationTests_ProduceConsumeExternalMessage : TestBase
	{
		private const int ReceiveDelay = 500;

		public AvroDeserilizationTests_ProduceConsumeExternalMessage()
		{
			Initialize<AvroClientBuilderConfigurator, AvroSiloBuilderConfigurator>(3);
		}

		[Fact]
		public async Task E2E()
		{
			var config = GetKafkaServerConfig();

			var testMessage = TestModelAvro.Random();

			var completion = new TaskCompletionSource<bool>();

			var provider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = provider.GetStream<TestModelAvro>(Consts.StreamNamespaceExternalAvro, Consts.StreamId4);

			await stream.QuickSubscribe((message, seq) =>
			{
				Assert.Equal(testMessage, message);
				completion.SetResult(true);
				return Task.CompletedTask;
			});

			await Task.Delay(5000);

			using (var schema = new CachedSchemaRegistryClient(new SchemaRegistryConfig
			{
				Url = "http://kafka-schema-registry.test-data:8081"
			}))
			using (var producer = new ProducerBuilder<byte[], TestModelAvro>(config)
				.SetValueSerializer(new AvroSerializer<TestModelAvro>(schema).AsSyncOverAsync())
				.Build()
			)
			{
				await producer.ProduceAsync(Consts.StreamNamespaceExternalAvro, new Message<byte[], TestModelAvro>
				{
					Key = Encoding.UTF8.GetBytes(Consts.StreamId4),
					Value = testMessage,
					Timestamp = new Timestamp(DateTimeOffset.UtcNow)
				});
			}

			await Task.WhenAny(completion.Task, Task.Delay(ReceiveDelay * 4));

			if (!completion.Task.IsCompleted)
				throw new XunitException("Message not received.");
		}

		private static ClientConfig GetKafkaServerConfig()
			=> new ClientConfig
			{
				BootstrapServers = string.Join(',', Brokers)
			};
	}

	public class AvroClientBuilderConfigurator : IClientBuilderConfigurator
	{
		public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
			=> clientBuilder
				.AddKafka(Consts.KafkaStreamProvider)
				.WithOptions(options =>
				{
					options.BrokerList = TestBase.Brokers;
					options.ConsumerGroupId = "E2EGroup_client";

					options
						.AddExternalTopic<TestModelAvro>(Consts.StreamNamespaceExternalAvro)
						;

					options.PollTimeout = TimeSpan.FromMilliseconds(10);
					options.ConsumeMode = ConsumeMode.StreamEnd;
				})
				.AddAvro("http://kafka-schema-registry.test-data:8081")
				.Build()
				;

	}

	public class AvroSiloBuilderConfigurator : ISiloConfigurator
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
						.AddExternalTopic<TestModelAvro>(Consts.StreamNamespaceExternalAvro)
						;
				})
				.AddAvro("http://kafka-schema-registry.test-data:8081")
				.AddLoggingTracker()
				.Build();
	}
}