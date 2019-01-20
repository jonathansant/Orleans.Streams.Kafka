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
