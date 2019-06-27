using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConfluentSample
{
	internal class Program
	{
		private static List<string> Brokers = new List<string>
		{
			"dev-data.rivertech.dev:39000",
			"dev-data.rivertech.dev:39001",
			"dev-data.rivertech.dev:39002"
		};

		private const string Topic = "sucrose-external";
		private const string RegistryUrl = "https://dev-data.rivertech.dev/schema-registry/";

		private static async Task Main(string[] args)
		{
			//			Task.Run(() => Consume());
			await Produce();
			//			await CreateTopic();

			Console.ReadKey();
		}

		private static async Task Produce()
		{
			try
			{
				var r = new Random();

				using (var schema = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = RegistryUrl }))
				using (var producer = new ProducerBuilder<byte[], AMessage>(new ProducerConfig
				{
					BootstrapServers = string.Join(',', Brokers)
				}).SetValueSerializer(new AvroSerializer<AMessage>(schema).AsSyncOverAsync()).Build())
				{
					var result = await producer.ProduceAsync(Topic, new Message<byte[], AMessage>
					{
						Key = Encoding.UTF8.GetBytes("streamId"),
						Value = new AMessage
						{
							id = 1,
							noOfHeads = r.Next(100)
						}
					});

					Console.WriteLine(
						$@"Delivered '{result.Value}' to: {result.TopicPartitionOffset}");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
			}
		}

		private static void Consume()
		{
			var conf = new ConsumerConfig
			{
				BootstrapServers = string.Join(',', Brokers),
				GroupId = "jonny-king-better-than-michael",
			};

			using (var schema = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = RegistryUrl }))
			using (var consumer = new ConsumerBuilder<string, AMessage>(conf).SetValueDeserializer(new AvroDeserializer<AMessage>(schema).AsSyncOverAsync()).Build())
			using (var admin = new AdminClientBuilder(conf).Build())
			{
				Console.WriteLine($@"Partition IDs: {
						string.Join(',',
						admin
						.GetMetadata(TimeSpan.FromMilliseconds(1000))
						.Topics
						.First(t => t.Topic.Contains(Topic))
						.Partitions
						.Select(x => x.PartitionId))
					}"
				);

				consumer.Assign(new[]
				{
					new TopicPartitionOffset(Topic, 1, Offset.Beginning),
					new TopicPartitionOffset(Topic, 2, Offset.Beginning),
					new TopicPartitionOffset(Topic, 3, Offset.Beginning)
				});
				//consumer.Subscribe(Topic);

				while (true)
				{
					var message = consumer.Consume(TimeSpan.FromMilliseconds(100));
					if (message != null)
					{
						Console.WriteLine(message.Value);
					}
				}
			}
		}

		private static async Task CreateTopic()
		{
			try
			{
				using (var admin = new AdminClientBuilder(new AdminClientConfig
				{
					BootstrapServers = string.Join(',', Brokers)
				}).Build())
				{
					admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.ForEach(t =>
					{
						Console.WriteLine(t.Topic);
						t.Partitions.ForEach(p => Console.WriteLine("              " + p.PartitionId));
					});

					await admin.CreateTopicsAsync(new[]
					{
						new TopicSpecification
						{
							Name = "TestTopicName",
							NumPartitions = 3,
							ReplicationFactor = 1
						},
						new TopicSpecification
						{
							Name = "TestTopicName3",
							NumPartitions = 3,
							ReplicationFactor = 1
						}
					});
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
			}
		}
	}
}