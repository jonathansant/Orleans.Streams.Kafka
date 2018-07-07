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
					//Produce();
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

			using (var producer = new Producer<string, string>(
				config, 
				new StringSerializer(Encoding.UTF8), 
				new StringSerializer(Encoding.UTF8))
			)
			{
				var publishPromise3 = producer.ProduceAsync("my-topic", "jello", "jelo jello jelllo the world");
				var publishPromise2 = producer.ProduceAsync("my-topic", "hello", "helloing the world");
				var publishPromise = producer.ProduceAsync("my-topic", "jonny", "test message text");
				var publishPromise4 = producer.ProduceAsync("my-topic", "dog-dragons", "dogzinskis");
				Task.WhenAll(publishPromise, publishPromise2, publishPromise3, publishPromise4).Wait();
				
				Console.WriteLine($@"Delivered '{publishPromise.Result.Value}' to: {publishPromise.Result.TopicPartitionOffset}");
				Console.WriteLine($@"Delivered '{publishPromise2.Result.Value}' to: {publishPromise2.Result.TopicPartitionOffset}");
				Console.WriteLine($@"Delivered '{publishPromise3.Result.Value}' to: {publishPromise3.Result.TopicPartitionOffset}");
				Console.WriteLine($@"Delivered '{publishPromise4.Result.Value}' to: {publishPromise4.Result.TopicPartitionOffset}");
			}
		}

		private static void Consume()
		{
			var conf = new Dictionary<string, object> 
			{ 
				{ "group.id", "test-consumer-group" },
				{ "bootstrap.servers", "localhost:9092" },
				{ "enable.auto.commit", false },
				//{ "auto.commit.interval.ms", 5000 },
//				{ "auto.offset.reset", "earliest" }
			};

			using (var consumer = new Consumer<string, string>(conf, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
			{
				Console.WriteLine($@"Partition IDs: {
						string.Join(',', 
						consumer
						.GetMetadata(false)
						.Topics
						.First(t => t.Topic.Contains("my-topic"))
						.Partitions
						.Select(x => x.PartitionId))
					}"
				);
				
				consumer.OnMessage += (_, msg)
					=>
				{
					Console.WriteLine($"Read '{msg.Value}' from: {msg.TopicPartitionOffset} Partition: {msg.Partition}");
					consumer.CommitAsync(msg).Wait();
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
				consumer.Assign(new []{ new TopicPartitionOffset("my-topic", 1, Offset.Stored)});

				while (true)
				{
					
				}

//				while (true)
//				{
//					consumer.Consume(out var mess, TimeSpan.FromMilliseconds(1));
//				}
			}
		}
	}
}