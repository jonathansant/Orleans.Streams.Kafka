using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConfluentSample
{
	internal class Program
	{
		private static async Task Main(string[] args)
		{
			//Task.Run(() => Consume());

			//			Task.Run(async () =>
			//			{
			//				while (true)
			//				{
			//					await Produce();
			//					await Task.Delay(100);
			//				}
			//			});

			await CreateTopic();
			//await Produce();
//			Consume();

			Console.ReadKey();
		}

		private static async Task Produce()
		{
			//			var config = new Dictionary<string, string>
			//					{
			//						{ "bootstrap.servers", "services.rivertech.dev:6800" },
			//		//				{"bootstrap.servers", "pkc-l9pve.eu-west-1.aws.confluent.cloud:9092"},
			//						{"api.version.request", "true"},
			//						{"broker.version.fallback", "0.10.0.0"},
			//						{"api.version.fallback.ms", "0"},
			//		//				{"sasl.mechanisms", "PLAIN"},
			//		//				{"security.protocol", "SASL_SSL"}
			//		//				{ "debug", "security,broker"}
			//					};

			try
			{
				using (var producer = new ProducerBuilder<byte[], string>(new ProducerConfig
				{
					BootstrapServers = "services.rivertech.dev:6800",
				}).Build())
				{
					var publishPromise5 = await producer.ProduceAsync("meraxesdog", new Message<byte[], string>
					{
						Key = Encoding.UTF8.GetBytes("streamId"),
						Value = "{ greeting: 'hello world' }",
						Headers = new Headers { new Header("external", BitConverter.GetBytes(true)) }
					});

					Console.WriteLine(
						$@"Delivered '{publishPromise5.Value}' to: {publishPromise5.TopicPartitionOffset}");
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
				BootstrapServers = "services.rivertech.dev:6800",
				//				BootstrapServers = "52.31.41.250:6800",
				GroupId = "jonny-king-better-than-michael",
			};
			
			using (var consumer = new ConsumerBuilder<string, string>(conf).Build())
			using (var admin = new AdminClientBuilder(conf).Build())
			{
				Console.WriteLine($@"Partition IDs: {
						string.Join(',',
						admin
						.GetMetadata(TimeSpan.FromMilliseconds(1000))
						.Topics
						.First(t => t.Topic.Contains("meraxesdog"))
						.Partitions
						.Select(x => x.PartitionId))
					}"
				);


				consumer.Assign(new TopicPartitionOffset("meraxesdog", 1, Offset.Beginning));
				//consumer.Subscribe("my-topic");

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
					BootstrapServers = "services.rivertech.dev:6800"
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
							Name = "meraxesdog",
							NumPartitions = 3,
							ReplicationFactor = 3
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