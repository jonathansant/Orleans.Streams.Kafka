## [6.2.0](https://github.com/jonathansant/orleans.streams.kafka/compare/6.1.1...6.2.0) (2022-02-13)

### Features

- Update ConfluentKafka 1.9.3
- Resolved incompatibility with Apple M1 processors

## [6.0.0](https://github.com/jonathansant/orleans.streams.kafka/compare/5.0.0...6.0.0) (2022-03-09)

### Features

- Update Orleans v3.6.0 & dotnet 6.0

### BREAKING CHANGES
- Update Orleans v3.6.0
- Update dotnet 6.0

## [5.0.0](https://github.com/jonathansant/orleans.streams.kafka/compare/4.2.0...5.0.0) (2021-12-15)

### Features

- Update Orleans v3.5.1 & Kafka v1.8.2

### BREAKING CHANGES
- Update Orleans v3.5.1
- Update Microsoft Extensions libraries v5.0.0

## [4.2.0](https://github.com/jonathansant/orleans.streams.kafka/compare/4.1.0...4.2.0) (2020-10-05)

### Features

- Add `ImportRequestContext` option

### Bug Fixes

- `ConsumerResult.Key` & `ConsumerResult.Value` where depricated and sometimes causing serialization issues

## [4.1.0](https://github.com/jonathansant/orleans.streams.kafka/compare/4.0.0...4.1.0) (2020-07-06)

### Features

- Add `ImportRequestContext` option

### Bug Fixes

- `ConsumerResult.Key` & `ConsumerResult.Value` where depricated and sometimes causing serialization issues

## [4.0.0](https://github.com/jonathansant/orleans.streams.kafka/compare/3.1.0...4.0.0) (2020-06-18)

### Features

- Update Orleans v3.2 & Kafka v1.4.3

### BREAKING CHANGES
- Update Orleans v3.2
- Update Microsoft Extensions libraries v3.1.5

## [3.1.0](https://github.com/jonathansant/orleans.streams.kafka/compare/3.0.0...3.1.0) (2020-03-12)

### Features

- Topics get created with retention period for messages in ms (Default is 7 days)

## [3.0.0](https://github.com/jonathansant/orleans.streams.kafka/compare/2.0.1...3.0.0) (2019-10-25)

### Features

- Topics get Auto Created with provided options
- Eager serialization: serialize eagerly instead of lazy in the batch container

### Bug Fixes

- Consumer not with lag 1 fix

### BREAKING CHANGES

- update to `Orleans.Streams.Utils` to version 8.0.0 which optimizes (and also fixes potential problems) message tracking
- `KafkaExternalBatchContainer` will fail on deserilization exceptions.


## [2.0.1](https://github.com/jonathansant/orleans.streams.kafka/compare/1.0.0...2.0.1) (2019-07-26)

### Features

- Add `KafkaStreamSiloBuilder`, `KafkaStreamClientBuilder`, `KafkaStreamSiloHostBuilder` for easier setup

### Bug Fixes

- Update fix from `Orleans.Stream.Utils` which fixes deserilization problems with message tracking

## [2.0.0](https://github.com/jonathansant/orleans.streams.kafka/compare/1.0.0...2.0.0) (2019-07-24)

### Features

- `AddJson`, `AddAvro` are no longer required if there are no external topics
- Add more tests and parallelize them

### BREAKING CHANGES

- update to `Orleans.Streams.Utils` to version 7.0.0 which optimizes (and also fixes potential problems) message tracking
- `KafkaExternalBatchContainer` will fail on deserilization exceptions.

## [1.0.0](https://github.com/jonathansant/orleans.streams.kafka/compare/0.9.1...1.0.0) (2019-07-05)

### Features

- **Avro Serialization:** add `AvroExternalDeserializer` for deserializing external messages using avro and regsitry schema
- **External Deserialization** integrate new IExternalDeserialization that are new in `Orleans.Steams.Utils` 6.0.0
- **deps** update to stable `Confluent.Kafka` Library

### BREAKING CHANGES

 - `Topic`
	- `Topic` is now `TopicConfig`.
 - `AddKafkaStreamProvider` requires `AddJson` or `AddAvro` to work

## [0.9.1](https://github.com/jonathansant/orleans.streams.kafka/compare/0.9.0...0.9.1) (2019-06-24)

### Features

- **builder:** add generic host support

## [0.9.0](https://github.com/jonathansant/orleans.streams.kafka/compare/0.8.0...0.9.0) (2018-06-12)

### Features

- `Topic` added to `KafkaStreamOptions`.

### BREAKING CHANGES

 - `KafkaStreamOptions`
	- `Topics` property now is of type `IList<Topic>` instead of `IList<string`.

## [0.8.0](https://github.com/jonathansant/orleans.streams.kafka/compare/0.7.1...0.8.0) (2019-03-25)

### BREAKING CHANGES

 - `Orleans`
	- updated to orleans 2.3.0.
 - `Orleans.Streams.Utils`
	- updated to `5.*`
 - `Confluent`
	- updated to `1.0.0`

## [0.7.1](https://github.com/jonathansant/orleans.streams.kafka/compare/0.7.0...0.7.1) (2019-02-7)

### Features

- update `Microsoft.Extensions.Logging` to 2.2.0

## [0.7.0](https://github.com/jonathansant/orleans.streams.kafka/compare/0.6.4...0.7.0) (2019-01-20)

### Features

- `KafkaAdapterReceiver` now can track a message using the new `MessageTrackerAPI` see: [`UseLoggingTracker`](https://github.com/jonathansant/Orleans.Streams.Kafka/blob/90982eaaf5c43dd6880cd3206c8eed9eb2ed9518/Orleans.Streams.Kafka/Core/KafkaAdapterReceiver.cs#L147). 

### BREAKING CHANGES

 - `Orleans`
	- updated to orleans 2.2.3.

## [0.6.4](https://github.com/jonathansant/orleans.streams.kafka/compare/0.6.3...0.6.4) (2019-01-19)

### Bug Fixes

- update `Orleans.Streams.Utils` to 2.1.1 which fixes Topics with one partition. 

## [0.6.3](https://github.com/jonathansant/orleans.streams.kafka/compare/0.6.2...0.6.3) (2018-12-08)

### Features

- added batching to `KafkaAdapterReceiver`.

## [0.6.2](https://github.com/jonathansant/orleans.streams.kafka/compare/0.6.0...0.6.2) (2018-11-22)

### Features

- removed `timeout` from the `Produce` extensions method. Producer timeout is now set via the `message.timeout.ms` Producer config.

## [0.6.0](https://github.com/jonathansant/orleans.streams.kafka/compare/0.4.0...0.6.0) (2018-11-19)

### Features

- `SsslCaLocations` added to `Credentials`. Ssl ca certificate location needs to be explicitly set by the application.

### BREAKING CHANGES

 - `KafkaStreamOptionsPublicExtensions`
	- `saslMechanisim` parameter in `WithSaslOptions` was changed to `saslMechanism`.

## [0.4.0](https://github.com/jonathansant/orleans.streams.kafka/compare/0.3.1...0.4.0) (2018-11-16)

### BREAKING CHANGES

 - `KafkaStreamOptions`
	- `WithConfluentCloudOptions` was changed to `WithSaslOptions`.

## [0.3.0](https://github.com/jonathansant/orleans.streams.kafka/compare/0.2.0...0.3.0) (2018-11-08)

### Features

 - **Confluent.Kafka:** bump `Confluent.Kafka` to version 1.0.0-beta2

### BREAKING CHANGES

 - `KafkaStreamOptions`
	- `InternallyManagedQueuesOnly` was removed.
