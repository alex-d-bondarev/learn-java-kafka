# Learning plan: Kafka with Java

Goal: understand Kafka well enough to build services with it and to answer test questions.

Environment: Java 25 (`25.0.1-amzn`, installed in SDKMAN), Docker, Gradle (SDKMAN). Build tool is Gradle (Kotlin DSL), so Gradle is a second thing you learn along the way.

Java 25 needs Gradle 9.1 or newer, and your SDKMAN Gradle is 8.10. This does not matter, because the project uses the Gradle wrapper (`./gradlew`), which downloads the version we pin. We also add a `.sdkmanrc` so `sdk env` selects Java 25 in this directory.

## How we will work

- One Gradle multi-project build. Each lesson is a subproject (`lesson-01-basics`, `lesson-02-producer`, ...). Dependencies are declared once in a version catalog (`gradle/libs.versions.toml`).
- Kafka runs locally in Docker (single broker in KRaft mode, no ZooKeeper) through a `docker-compose.yml`. Tests use Testcontainers so they start their own broker.
- Each lesson has three parts:
  1. Short explanation of the concept (a `README.md` in the lesson folder).
  2. Runnable code, small enough to read in one sitting.
  3. Exercises for you to do, plus the test questions that lesson covers. Answers come after you try.
- Gradle is taught on demand. Each lesson introduces one Gradle concept it needs (see "Gradle track").

## Part 1: Kafka topics and lessons

### Lesson 0: Setup
- Gradle wrapper, multi-project layout, version catalog, `application` plugin, `./gradlew :lesson-01:run`.
- Kafka in Docker, CLI tools (`kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh`).

### Lesson 1: Core concepts
- Event, topic, partition, offset, broker, cluster.
- Log as append-only storage, retention (time and size), log compaction.
- Why Kafka differs from a queue (RabbitMQ) and from a database.
- KRaft vs ZooKeeper, controller role.

### Lesson 2: Producer
- `KafkaProducer`, `ProducerRecord`, serializers.
- Keys and partitioning: default partitioner, ordering per key, custom partitioner.
- `acks` (0, 1, all), `retries`, `delivery.timeout.ms`, `linger.ms`, `batch.size`, compression.
- Sync vs async send, callbacks, error handling.
- Idempotent producer and why it exists.

### Lesson 3: Consumer
- `KafkaConsumer`, poll loop, deserializers.
- Consumer groups, partition assignment, rebalancing (eager vs cooperative).
- Offsets: auto vs manual commit, `commitSync` vs `commitAsync`, `auto.offset.reset`.
- `max.poll.interval.ms`, `session.timeout.ms`, heartbeats, why consumers get kicked out.
- Seeking, replaying, static membership.

### Lesson 4: Delivery guarantees
- At-most-once, at-least-once, exactly-once.
- Idempotent consumers and deduplication.
- Transactions: transactional producer, `read_committed`, consume-transform-produce loop.
- Where exactly-once stops (side effects outside Kafka).

### Lesson 5: Replication and reliability
- Replication factor, leader and followers, ISR, high watermark.
- `min.insync.replicas` together with `acks=all`.
- Leader election, unclean leader election, what happens when a broker dies.
- Durability vs availability trade-offs.

### Lesson 6: Serialization and schemas
- JSON with Jackson, Avro or Protobuf, Schema Registry.
- Schema evolution: backward, forward, full compatibility.
- Poison pills and how to survive them.

### Lesson 7: Error handling patterns
- Retries, retry topics, dead letter topic.
- Handling deserialization errors.
- Backpressure, `pause` and `resume`.

### Lesson 8: Admin API and operations
- `AdminClient`: create and describe topics, consumer group lag.
- Partition count choices (and why you cannot reduce them).
- Key metrics: consumer lag, under-replicated partitions, request latency.
- Basic security vocabulary: TLS, SASL, ACLs.

### Lesson 9: Kafka Streams
- KStream vs KTable, stateless and stateful operations.
- Windowing, joins, state stores, repartitioning.
- When to use Streams vs a plain consumer.

### Lesson 10: Spring Kafka and Kafka Connect
- `@KafkaListener`, `KafkaTemplate`, error handlers, `DefaultErrorHandler`.
- Connect: source and sink connectors, when it replaces custom code.
- Testing: Testcontainers, `EmbeddedKafka`.

### Lesson 11: Capstone
- Small order-processing system: producer service, consumer service, retry and dead letter topics, idempotent processing, integration test with Testcontainers.

## Part 2: Gradle track

Introduced inside the lessons above, in this order:

1. Wrapper, `settings.gradle.kts`, `build.gradle.kts`, tasks (lesson 0).
2. Java toolchain (pin Java 25), `.sdkmanrc`, wrapper version bump to 9.1+, `application` plugin, `run` task (lesson 0).
3. Dependencies and configurations: `implementation`, `testImplementation`, `runtimeOnly` (lesson 1).
4. Version catalog `libs.versions.toml` (lesson 1).
5. Multi-project builds and shared config via `subprojects {}` or a convention plugin in `buildSrc` (lesson 2).
6. Test setup: JUnit 5, `useJUnitPlatform()`, Testcontainers (lesson 4).
7. Build cache, `--info`, `dependencies` and `dependencyInsight` tasks for debugging (lesson 6).
8. Source sets for integration tests (lesson 10).

## Part 3: Test questions

Questions are grouped by level. We answer each one in the matching lesson, with code where it helps.

### Fundamentals
- What is a topic, a partition and an offset? How do they relate?
- What guarantees does Kafka give about ordering?
- How does Kafka differ from RabbitMQ or a database?
- What role do brokers and the controller play?
- How does retention work, and what is log compaction?

### Producer
- How does a record get assigned to a partition?
- What do `acks=0`, `1` and `all` mean? Which would you pick for payments?
- What does `enable.idempotence` fix? What can still go wrong without it?
- How do `linger.ms` and `batch.size` trade latency for throughput?
- How do you keep ordering when retries are on?

### Consumer
- How do consumer groups work? What happens with more consumers than partitions?
- What triggers a rebalance? How do you reduce its impact?
- Auto commit vs manual commit: which one risks duplicates, which one risks loss?
- A consumer keeps being kicked out of the group. What do you check?
- How do you reprocess messages from a given time?

### Reliability and guarantees
- Explain at-most-once, at-least-once and exactly-once. How does Kafka reach exactly-once?
- What are ISR and `min.insync.replicas`? What happens with replication factor 3, `min.insync.replicas=2` and two brokers down?
- What is unclean leader election, and when would you enable it?
- How do you make a consumer idempotent?

### Design and operations
- How do you choose the number of partitions?
- How do you handle a poison pill message?
- How do you design retry and dead letter handling without blocking a partition?
- How would you monitor a Kafka-based system? What does growing consumer lag mean?
- How do you evolve a message schema without breaking consumers?
- Kafka Streams vs a plain consumer: when each?
- Walk through designing: "process 50k orders per second with no loss and no duplicates."

## Progress

| Lesson | Status |
|---|---|
| 0 Setup | not started |
| 1 Core concepts | not started |
| 2 Producer | not started |
| 3 Consumer | not started |
| 4 Delivery guarantees | not started |
| 5 Replication | not started |
| 6 Serialization | not started |
| 7 Error handling | not started |
| 8 Admin and ops | not started |
| 9 Kafka Streams | not started |
| 10 Spring Kafka and Connect | not started |
| 11 Capstone | not started |

## Next step

Lesson 0: I scaffold the Gradle multi-project build, `docker-compose.yml` for a KRaft broker, and a first producer and consumer. Tell me if you want to reorder lessons or skip any (Spring and Streams are the most skippable if your target roles use plain clients).
