# Orleans.Stream.Kafka
Kafka persistent stream provider for Microsoft Orleans that uses the [Confluent SDK](https://github.com/confluentinc/confluent-kafka-dotnet).

[![NuGet version](https://badge.fury.io/nu/orleans.streams.kafka.svg)](https://badge.fury.io/nu/orleans.streams.kafka)

# Dependencies
`Orleans.Streams.Kafka` has the following dependencies:
* Confluent.Kafka: **1.0.0-beta-2**
* Orleans.Streams.Utils: [![NuGet version](https://badge.fury.io/nu/Orleans.Streams.Utils.svg)](https://badge.fury.io/nu/Orleans.Streams.Utils)

## Installation
To start working with the `Orleans.Streams.Kafka` make sure you do the following steps:

1. Install Kafka on a machine (or cluster) which you have access to use the [Confluent Platform](https://www.confluent.io/download/).
2. Create Topics in Kafka with as many partitions as needed for each topic.
3. Install the `Orleans.Streams.Kafka` nuget from the nuget repository.
4. Add to the Silo configuration the new stream provider with the necessary parameters and the optional ones (if you wish). you can see what is configurable in KafkaStreamProvider under [Configurable Values](#configurableValues).

Example KafkaStreamProvider configuration: 
```CSharp
public class SiloBuilderConfigurator : ISiloBuilderConfigurator
{
	public void Configure(ISiloHostBuilder hostBuilder)
		=> hostBuilder
			.AddMemoryGrainStorage("PubSubStore")
			.AddKafkaStreamProvider("KafkaStreamProvider", options =>
			{
				options.BrokerList = new List<string> { "localhost:9092" };
				options.ConsumerGroupId = "TestGroup";
				options.PollTimeout = TimeSpan.FromMilliseconds(10);

				options
						.AddTopic("topic1")
						.AddTopic("topic2");
			});
}

public class ClientBuilderConfigurator : IClientBuilderConfigurator
{
	public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
		=> clientBuilder
			.AddKafkaStreamProvider("KafkaStreamProvider", options =>
			{
				options.BrokerList = new List<string> { "localhost:9092" };
				options.ConsumerGroupId = "TestGroup";
				options.PollTimeout = TimeSpan.FromMilliseconds(10);

        options
						.AddTopic("topic1")
						.AddTopic("topic2");
			});
}
```

## Usage

### Producing:
```CSharp
var testGrain = clusterClient.GetGrain<ITestGrain>(grainId);

var result = await testGrain.GetThePhrase();

Console.BackgroundColor = ConsoleColor.DarkMagenta;
Console.WriteLine(result);

var streamProvider = clusterClient.GetStreamProvider("KafkaProvider");
var stream = streamProvider.GetStream<TestModel>("streamId", "topic1");
await stream.OnNextAsync(new TestModel
{
	Greeting = "hello world"
});
```

### Consuming:
```CSharp
var kafkaProvider = GetStreamProvider("KafkaStreamProvider");
var testStream = kafkaProvider.GetStream<TestModel>("streamId", "topic1");

// To resume stream in case of stream deactivation
var subscriptionHandles = await testStream.GetAllSubscriptionHandles();

if (subscriptionHandles.Count > 0)
{
	foreach (var subscriptionHandle in subscriptionHandles)
	{
		await subscriptionHandle.ResumeAsync(OnNextTestMessage);
	}
}

await testStream.SubscribeAsync(OnNextTestMessage);
```

*Note: The Stream Namespace identifies the **Kafka topic**.*

## <a name="configurableValues"></a>Configurable Values
These are the configurable values that the `Orleans.Streams.Kafka`:

- **Topics**: The topics that will be used where messages will be Produced/Consumed.
- **BrokerList**: List of Kafka brokers to connect to.
- **ConsumerGroupId**: The ConsumerGroupId used by the Kafka Consumer. *Default value is `orleans-kafka`*
- **PollTimeout**: Determines the duration that the Kafka consumer blocks for to wait for messages. *Default value is `100ms`*
- **PollBufferTimeout**: Determines the duration the `KafkaAdapterReceiver` will continue to poll for messages (for the same batch) *Default value is `500ms`*
- **AdminRequestTimeout**: Timeout for admin requests. *Default value is `5s`*
- **ConsumeMode**: Determines the offset to start consuming from. *Default value is `ConsumeMode.LastCommittedMessage`*
- **ProducerTimeout**: Timeout for produce requests. *Default value is `5s`*
