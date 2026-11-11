# Kafka Order Microservices

Three independent Java services demonstrate HTTP order publication, consumer notification, Kafka Streams processing, JSON Schema serialization and Kafka Connect file export. Objectives include asynchronous publishing, independent service deployment, a JSON contract, keyed ordering, acknowledgments, consumer groups, validation, recovery, and automated testing.

## Architecture

```mermaid
flowchart LR
    Client["Swagger UI / HTTP client"] -->|"POST /api/orders JSON"| Producer["Producer :8081"]
    Producer -->|"key = orderId; JSON event"| Kafka[("Kafka :9092<br/>orders.created.v2")]
    Kafka -->|"order-notification-v2-group"| Consumer["Consumer :8082"]
    Consumer --> Logs["Order notification log"]
    Kafka -->|"broker acknowledgment"| Producer
    Producer -->|"202 + topic, partition, offset"| Client
```

A 202 response confirms the broker acknowledged the event. Consumer processing happens independently; 202 does not confirm a notification was processed. There is no order database or actual email provider in this assignment.

## Technology stack

- Java 21, Maven (independent POMs and Maven wrappers).
- Spring Boot 4.1.2 and its managed Spring Kafka 4.1.2 / Kafka client 4.2.2 dependencies.
- Apache Kafka **broker** 4.1.2 in Docker, using KRaft (no ZooKeeper).
- Spring Web MVC, Actuator, Jakarta Bean Validation (producer).
- springdoc-openapi 3.2.0 and Swagger UI (producer).
- Jackson 3 via Spring Kafka's `JacksonJsonSerializer` and `JacksonJsonDeserializer`.
- JUnit, Mockito, MockMvc and Spring Boot test support.

The broker and client have different version numbers by design: the client version comes from Spring Boot dependency management. Avoid independently downgrading Kafka or replacing these serializers with deprecated Jackson 2 APIs. References: [Spring Kafka JSON serialization](https://docs.spring.io/spring-kafka/reference/kafka/serdes.html), [springdoc compatibility](https://springdoc.org/#what-is-the-compatibility-matrix-of-springdoc-openapi-with-spring-boot).

## Project structure

```text
KafkaMicroservices/
  compose.yaml
  README.md
  .gitignore
  producer-service/
    pom.xml, mvnw, mvnw.cmd
    src/main/java/com/henry/producerservice/
      ProducerServiceApplication.java
      controller/OrderController.java
      controller/GlobalExceptionHandler.java
      event/OrderCreatedEvent.java
      service/OrderProducerService.java
      service/OrderPublicationException.java
    src/main/resources/application.yaml
    src/test/java/com/henry/producerservice/...
  consumer-service/
    pom.xml, mvnw, mvnw.cmd
    src/main/java/com/henry/consumer/
      ConsumerServiceApplication.java
      config/KafkaConsumerConfiguration.java
      event/OrderCreatedEvent.java
      service/OrderConsumerService.java
    src/main/resources/application.yaml
    src/test/java/com/henry/consumer/...
```

Each public class lives in its matching file. Both services own their event record; their Java package names differ, but their JSON fields match. No shared Java library is needed. Type headers are disabled on publication and ignored during consumption; the consumer explicitly chooses its own record type.

## Prerequisites and Docker

Run commands from the root in PowerShell:

```powershell
Set-Location 'C:\Users\egede\OneDrive\Desktop\KafkaMicroservices'
java -version
docker compose ps
docker compose start kafka
```

For a **new installation only**, create the broker with `docker compose up -d kafka`. The existing Compose file binds `127.0.0.1:9092`; the verified broker advertises `localhost:9092`, which is correct for services running on the host.

Inspect topics and group progress:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.created.v2
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-notification-v2-group
```

Only on a fresh broker where the JSON topic is missing:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.created.v2 --partitions 1 --replication-factor 1
```

Preserve the existing container and volumes. Use `docker compose stop kafka` and `docker compose start kafka` to stop/start without replacing it. The image currently uses anonymous volumes, so recreating containers can detach existing data. Do not run `down -v`, remove volumes/topics, or reset offsets. The original `orders.created` plain-text topic is left intact; JSON consumers subscribe only to `orders.created.v2`.

## Build and run

Build each service (there is no root aggregator POM):

```powershell
.\producer-service\mvnw.cmd -f producer-service/pom.xml verify
.\consumer-service\mvnw.cmd -f consumer-service/pom.xml verify
```

In IntelliJ, reload both Maven projects after dependency changes. Run `ProducerServiceApplication` and `ConsumerServiceApplication` using Java 21. Stop older instances first if ports are occupied.

Alternatively run these in two separate PowerShell terminals from the root:

```powershell
java -jar producer-service/target/producer-service-0.0.1-SNAPSHOT.jar
```

```powershell
java -jar consumer-service/target/consumer-service-0.0.1-SNAPSHOT.jar
```

The consumer can start independently, including before the producer. Kafka must be available for message delivery; if unavailable the listener reconnects. Actuator HTTP liveness alone does not prove a Kafka connection.

- Producer health: http://localhost:8081/actuator/health
- Consumer health: http://localhost:8082/actuator/health
- Swagger UI: http://localhost:8081/swagger-ui/index.html
- OpenAPI JSON: http://localhost:8081/v3/api-docs

## Demonstration request

In Swagger select `POST /api/orders`, click **Try it out**, paste the JSON, and click **Execute**:

```json
{
  "orderId": "ORD-9010",
  "customerId": "CUST-101",
  "product": "Laptop",
  "quantity": 2,
  "status": "CREATED"
}
```

Equivalent PowerShell:

```powershell
$body = @{
    orderId = 'ORD-9010'
    customerId = 'CUST-101'
    product = 'Laptop'
    quantity = 2
    status = 'CREATED'
} | ConvertTo-Json
Invoke-WebRequest -Uri 'http://localhost:8081/api/orders' -Method Post -ContentType 'application/json' -Body $body -UseBasicParsing |
    Select-Object StatusCode, Content
```

Expected HTTP 202, for example (offset varies):

```json
{"orderId":"ORD-9010","status":"PUBLISHED","topic":"orders.created.v2","partition":0,"offset":7}
```

The producer logs `Kafka acknowledged orderId=ORD-9010 topic=orders.created.v2 partition=0 offset=...`.
The consumer Run console logs:
```text
Order notification processed successfully: orderId=ORD-9010 customerId=CUST-101 product=Laptop quantity=2 status=CREATED
```

Use the **ConsumerServiceApplication** Run tab in IntelliJ and search for the order ID. If running a JAR and saving logs, start it with `--logging.file.name=consumer-service/target/demo-consumer.log`, then monitor:

```powershell
Get-Content consumer-service/target/demo-consumer.log -Tail 30 -Wait
```

Group lag should eventually be zero after successful consumption. Reusing an order ID sends another event; this demo has no application-level deduplication.

## Validation and error handling

The producer uses constructor injection, `@Valid`, `@NotBlank`, `@NotNull`, `@Positive`, and an exact `CREATED` status constraint. Quantity must be present and positive. Invalid requests never reach the Kafka service.

| Outcome | HTTP response |
| --- | --- |
| Kafka acknowledges publication | 202 JSON acknowledgment |
| Missing/blank fields, nonpositive quantity, invalid status | 400 Problem Detail with field errors |
| Malformed JSON | 400 Problem Detail |
| Unsupported content type | 415 Problem Detail |
| Kafka send/acknowledgment failure | 503 Problem Detail |
| Unexpected application failure | 500 Problem Detail |

The global handler uses Spring's `ProblemDetail` responses and avoids exposing internal exception details. Producer sends return a `CompletableFuture`; no blocking `get()` or `join()` is used in request handling. Kafka metadata acquisition can block up to 5 seconds; delivery is bounded to 10 seconds and the MVC async timeout is 20 seconds. The producer requests `acks=all` and enables Kafka producer idempotence. A 503 can have an ambiguous delivery outcome, so verify before retrying; idempotence does not deduplicate separate HTTP requests.

The consumer wraps Jackson JSON deserialization in `ErrorHandlingDeserializer` so malformed records reach the container error handler. Valid JSON is also checked for required business values before logging success. Transient listener failures get two retries with one-second backoff. Invalid events and deserialization failures are nonretryable. After recovery, the failed record location and exception are logged and consumption continues; record acknowledgment mode and disabled auto-commit let the container manage offsets.

**Recovery policy:** exhausted/invalid records are skipped after logging. There is no dead-letter topic or durable failed-record archive in this assignment. The original record remains in the source topic until Kafka retention removes it, but this consumer group advances past it. For production, add a dead-letter topic and idempotent business processing.

## Automated tests

```powershell
.\producer-service\mvnw.cmd -f producer-service/pom.xml verify
.\consumer-service\mvnw.cmd -f consumer-service/pom.xml verify
```

A locally installed Maven can replace the wrapper, e.g. `mvn -f producer-service/pom.xml verify`.

Tests use mock Kafka templates/consumers and disable listener startup in context tests. No live broker is required. They cover API success, waiting for acknowledgment, invalid/missing input, malformed JSON, 415/500/503 handling, correct topic/key, synchronous/asynchronous publishing failures, consumer field logging and validation, Jackson 3 deserialization, malformed-record capture, retry exhaustion, and nonretryable recovery.

Surefire reports are written to each service's `target/surefire-reports`. Live end-to-end testing is a separate check using the demonstration above. See [verification results](VERIFICATION.md) for actual executed results and limitations.

## Kafka concepts demonstrated

- **Topic:** a durable named event stream; v2 separates JSON from older text messages.
- **Producer and broker acknowledgment:** successful send confirms Kafka stored the record under the configured acknowledgment policy.
- **Key and partition:** order ID is the key; records with the same key go to the same partition under stable partitioning.
- **Offset:** record position within a partition, included in the publication response.
- **Consumer group:** `order-notification-v2-group` tracks its progress and shares partitions between group members.
- **Offset reset:** `earliest` applies only when the group has no valid committed offset; normal restarts resume from commits.
- **Delivery:** processing is at least once; failures/restarts can cause duplicates.
- **Single broker:** replication factor 1 suits a local demonstration but does not survive broker/data loss.
- **Loose coupling:** producer and consumer communicate through a JSON event contract and can run separately.

## Git and excluded files

The existing repository is https://github.com/egedegbehenry/spring-boot-kafka-microservices.git. Completed advanced integration is on feature/kafka-advanced-integration. Root and service ignore files exclude build outputs, IDE state and secrets. Generated Connect output and the empty accidental PowerShell artifact -Uri remain local.


## Advanced integration and migration to a new computer

The original architecture diagram above shows the producer/consumer path. The completed extension adds:

```text
orders.created.v2 -> order-processing-streams
  -> orders.processed.v1 (plain JSON) -> orders-file-sink -> connect-output/orders-processed.jsonl
  -> orders.processed.schema.v1 (Confluent JSON Schema framing, Registry subject orders.processed.schema.v1-value)
```

The Streams filter requires a nonblank orderId and product, positive quantity and status CREATED. It preserves the key and maps to orderId, product, quantity and processingStatus PROCESSED. The consumer still receives the original created events. No topic names, application IDs, serializers or business rules have been changed for this backup.

Additional versioned components: streams-service/ (POM, wrappers, .mvn, source, application.yaml and tests), schemas/processed-order.schema.json, connectors/orders-file-sink.json, and the Schema Registry/Connect services and localhost proxies in compose.yaml.

### Install and clone

Install a **JDK 21**, Git, and Docker Desktop with its Linux container engine and Docker Compose enabled. Set JAVA_HOME to the JDK and check java -version. Standalone Maven is optional: all three services include Maven wrappers, which download Maven and dependencies on first use (internet access required).

Download IntelliJ IDEA from [JetBrains](https://www.jetbrains.com/idea/download/) and run its installer or install through JetBrains Toolbox; see the [official installation guide](https://www.jetbrains.com/help/idea/installation-guide.html). Open the cloned directory, select JDK 21 as the Project SDK, and link/import all three service pom.xml files as Maven projects. Recreate run configurations for their application main classes with the matching service as the module.

Run in PowerShell from your chosen parent folder:

```powershell
git clone --branch feature/kafka-advanced-integration https://github.com/egedegbehenry/spring-boot-kafka-microservices.git
Set-Location spring-boot-kafka-microservices
New-Item -ItemType Directory -Force connect-output | Out-Null
docker compose up -d
docker compose ps
```

These startup instructions apply to a fresh clone. Keep existing old-computer containers/volumes intact. Services may need time to initialize; check docker compose logs schema-registry kafka-connect if endpoints are not ready. Shared Kafka networking and the two proxies expose Registry on localhost:8085 and Connect on localhost:8083. Kafka is localhost:9092. The bind mount ./connect-output:/data stores exported records on the host; .gitkeep preserves the directory in Git.

### Restore topics and schema before starting Streams

On a fresh broker, create the three application topics:

```powershell
foreach ($topic in @('orders.created.v2', 'orders.processed.v1', 'orders.processed.schema.v1')) {
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
}
Invoke-RestMethod -Uri 'http://localhost:8085/subjects'
$schemaRegistration = @{
    schemaType = 'JSON'
    schema = Get-Content schemas/processed-order.schema.json -Raw
} | ConvertTo-Json -Depth 20
Invoke-RestMethod -Uri 'http://localhost:8085/subjects/orders.processed.schema.v1-value/versions' -Method Post -ContentType 'application/vnd.schemaregistry.v1+json' -Body $schemaRegistration
Invoke-RestMethod -Uri 'http://localhost:8085/subjects/orders.processed.schema.v1-value/versions/latest'
```

The previous successful integration used Schema ID 1 (historical result supplied with the assignment). A new Registry may assign a different ID; do not hardcode 1. Streams uses the latest registered subject version with automatic registration disabled and schema validation enabled. Register before sending events. Existing application.yaml uses the localhost:8085 default in the Java @Value expression; its nested app block is under Streams properties. For a different Registry address, pass the top-level property --app.schema-registry.url=http://HOST:PORT explicitly.

### Register the FileStream sink

Wait until Connect responds, then register the reviewed configuration:

```powershell
Invoke-RestMethod -Uri 'http://localhost:8083/connector-plugins'
Invoke-RestMethod -Uri 'http://localhost:8083/connectors' -Method Post -ContentType 'application/json' -Body (Get-Content connectors/orders-file-sink.json -Raw)
Invoke-RestMethod -Uri 'http://localhost:8083/connectors/orders-file-sink/status'
```

Expect connector and task state RUNNING. If already registered (HTTP 409), inspect its existing configuration and status rather than deleting it. The configured plugin path includes /usr/share/filestream-connectors and the connector reads **orders.processed.v1** with StringConverter. The schema-aware topic deliberately has a different wire format.

### Build, test and run all three services

From the repository root:

```powershell
.\producer-service\mvnw.cmd -f producer-service/pom.xml verify
.\consumer-service\mvnw.cmd -f consumer-service/pom.xml verify
.\streams-service\mvnw.cmd -f streams-service/pom.xml verify
```

After successful builds, start each in a separate terminal (or run ProducerServiceApplication, ConsumerServiceApplication and StreamsServiceApplication in IntelliJ):

```powershell
java -jar producer-service/target/producer-service-0.0.1-SNAPSHOT.jar
```

```powershell
java -jar consumer-service/target/consumer-service-0.0.1-SNAPSHOT.jar
```

```powershell
java -jar streams-service/target/streams-service-0.0.1-SNAPSHOT.jar
```

Producer runs on 8081, consumer on 8082; Streams is non-web. Use Swagger at http://localhost:8081/swagger-ui/index.html and submit the demonstration request above with a new orderId. Expect HTTP 202, consumer notification logs, and processed output:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.processed.v1 --from-beginning --max-messages 1 --timeout-ms 15000
Get-Content connect-output/orders-processed.jsonl -Tail 10
docker compose exec schema-registry kafka-json-schema-console-consumer --bootstrap-server localhost:9092 --topic orders.processed.schema.v1 --from-beginning --max-messages 1 --timeout-ms 15000 --property schema.registry.url=http://localhost:8085
```

The last command decodes the Confluent framing using Registry (see [Confluent JSON Schema documentation](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-json.html)). An ordinary string console consumer cannot properly decode this topic. Reading from the beginning may show an older record; match the new orderId using a longer console read if needed. The demo output file is intentionally ignored and recreated by the sink as new processed orders arrive. Restarting an existing connector resumes its offsets; removing its output file does not itself replay old messages.

### Separate backup before retiring the old computer

Git preserves source, schemas, configuration, tests and documentation. It does **not** preserve running applications, Kafka messages/offsets, Registry contents, Connect runtime state or Docker volumes. The broker's current Compose configuration has no explicit named data volume; anonymous/container data needs a separate Docker-aware backup if historical events matter. Preserve the existing containers and volumes until that backup is checked. Do not use down -v, remove topics or reset offsets.

The local connect-output/orders-processed.jsonl contains demo/test events and is reproducible by sending test orders. Copy it separately if exact historical records matter. The zero-byte -Uri file has no project content. Ignored service HELP.md files are generated Spring Initializr reference links. target/ contains reproducible builds, reports and demo logs; preserve any historical evidence separately if required.

Export IntelliJ settings/run configurations securely if desired (.idea is excluded). Transfer any personal Maven settings/proxy credentials, Git SSH keys or credential-manager setup, Docker configuration and environment variables via a secure method, never this repository. No .env or private-key files were found in the project file inventory. Check personal files outside this project separately before retiring the machine.

A pull request and merge to main are optional later; cloning the feature branch is sufficient to recover this completed project.
