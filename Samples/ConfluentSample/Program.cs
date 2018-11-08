using Confluent.Kafka;
using System;
using System.Collections.Generic;
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

			Task.Run(async () =>
			{
				while (true)
				{
					await Produce();
					await Task.Delay(100);
				}
			});

			Console.ReadKey();
		}

		private static async Task Produce()
		{
			var config = new Dictionary<string, string>
			{
				//{ "bootstrap.servers", "localhost:9092" }
				{"bootstrap.servers", "pkc-l9pve.eu-west-1.aws.confluent.cloud:9092"},
				{"api.version.request", "true"},
				{"broker.version.fallback", "0.10.0.0"},
				{"api.version.fallback.ms", "0"},
				{"sasl.mechanisms", "PLAIN"},
				{"security.protocol", "SASL_SSL"}
//				{ "debug", "security,broker"}
			};

			try
			{
				using (var producer = new Producer<byte[], string>(config))
				{
					var publishPromise5 = await producer.ProduceAsync("jonnyenglish", new Message<byte[], string>
					{
						Key = Encoding.UTF8.GetBytes("streamId"),
						Value = "{ greeting: 'hello world' }",
						Headers = new Headers {new Header("external", BitConverter.GetBytes(true))}
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
			var conf = new Dictionary<string, string>
			{
				{ "group.id", "test-consumer-group" },
				{ "bootstrap.servers", "localhost:9092" },
				{ "enable.auto.commit", "false" },
				//{ "auto.commit.interval.ms", 5000 },
//				{ "auto.offset.reset", "earliest" }
			};

			using (var consumer = new Consumer<string, string>(conf))
			using (var admin = new AdminClient(consumer.Handle))
			{
				Console.WriteLine($@"Partition IDs: {
						string.Join(',',
						admin
						.GetMetadata(TimeSpan.FromMilliseconds(1000))
						.Topics
						.First(t => t.Topic.Contains("my-topic"))
						.Partitions
						.Select(x => x.PartitionId))
					}"
				);

				consumer.OnPartitionEOF += (_, end)
					=> Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");


				consumer.Assign(new TopicPartitionOffset("my-topic", 1, Offset.Beginning));
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
	}
}