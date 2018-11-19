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