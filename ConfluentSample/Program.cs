using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace ConfluentSample
{
	class Program
	{
		static void Main(string[] args)
		{
			Task.Run(() => Consume());

			Task.Run(async () => {
				while (true)
				{
				//	Produce();
					await Task.Delay(100);
				}
			});

			Console.ReadKey();
		}

		private static void Produce()
		{
			var config = new Dictionary<string, object> 
			{ 
				{ "bootstrap.servers", "localhost:9092" } 
			};

			using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
			{
				var publishPromise = producer.ProduceAsync("my-topic", null, "test message text");
				publishPromise.Wait();
				
				Console.WriteLine($"Delivered '{publishPromise.Result.Value}' to: {publishPromise.Result.TopicPartitionOffset}");
			}
		}

		private static void Consume()
		{
			var conf = new Dictionary<string, object> 
			{ 
				{ "group.id", "test-consumer-group" },
				{ "bootstrap.servers", "localhost:9092" },
				{ "enable.auto.commit", true },
				{ "auto.commit.interval.ms", 5000 },
//				{ "auto.offset.reset", "earliest" }
			};

			using (var consumer = new Consumer<Null, string>(conf, null, new StringDeserializer(Encoding.UTF8)))
			{
				consumer.OnPartitionsAssigned += (_, partitions) => Console.WriteLine(string.Join(',', partitions.Select(x => x.Partition)));
				
				consumer.Assign(new List<TopicPartitionOffset>{new TopicPartitionOffset("my-topic", 0, Offset.Stored)});
				
				consumer.OnMessage += (_, msg)
					=>
				{
					Console.WriteLine($"Read '{msg.Value}' from: {msg.TopicPartitionOffset} Partition: {msg.Partition}");
				};

				consumer.OnError += (_, error)
					=> Console.WriteLine($"Error: {error}");

				consumer.OnConsumeError += (_, msg)
					=> Console.WriteLine($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");
				
				consumer.OnPartitionEOF += (_, end)
					=> Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");
				
				consumer.OnConsumeError += (_, msg)
					=> Console.WriteLine($"Error consuming from topic/partition/offset {msg.Topic}/{msg.Partition}/{msg.Offset}: {msg.Error}");

				//consumer.Subscribe("my-topic");

				while (true)
				{
					consumer.Poll(TimeSpan.FromMilliseconds(100));
				}

//				while (true)
//				{
//					consumer.Consume(out var mess, TimeSpan.FromMilliseconds(1));
//				}
			}
		}
	}
}