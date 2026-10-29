# Verification and change record

Executed on 29 October 2026 in the existing Windows workspace using Java 21.0.12.1, Maven 3.10.0-rc-1 and Docker. These results describe executed checks, not proposed tests.

## Builds and automated tests

| Command | Result | Tests | Failures / errors / skipped |
| --- | --- | --- | --- |
| `mvn -f producer-service/pom.xml -B verify` | BUILD SUCCESS | 12 | 0 / 0 / 0 |
| `mvn -f consumer-service/pom.xml -B verify` | BUILD SUCCESS | 7 | 0 / 0 / 0 |

Both executable JARs were packaged successfully. Ordinary tests use mocked Kafka facilities and disable live listener startup in application context tests.

Producer tests: 8 controller tests, 3 publishing service tests, 1 context test.
Consumer tests: 4 serialization/recovery tests, 2 event-processing tests, 1 context test.

Raw evidence (generated, ignored by Git):
- `producer-service/target/build-verification.log`
- `consumer-service/target/build-verification.log`
- Each service's `target/surefire-reports/`

Mockito emitted a Java agent attachment warning on Java 21; it did not fail tests. IntelliJ inspections of the repaired entry point, controller and exception handler reported no errors, with minor unused-parameter/nullability warnings in the handler.

## Live integration

The pre-existing Kafka container `kafkamicroservices-kafka-1` was preserved, with its original container ID `4846cc3d5d22`. No Docker configuration, topic, volume, or offset reset operation was performed. Consumer offsets advanced normally by processing the new demonstration message.

Confirmed:
- Docker publishes `127.0.0.1:9092->9092`.
- Broker advertises `PLAINTEXT://localhost:9092`.
- Existing `orders.created.v2`: one partition, replication factor one.
- Producer starts on 8081 and consumer independently on 8082.
- Both `/actuator/health` responses: `UP`.
- Swagger UI HTML: HTTP 200.
- OpenAPI JSON loads and contains `POST /api/orders`.
- HTTP order request through the producer reaches Kafka and the consumer.

The existing IntelliJ-launched application instances were identified by their exact main classes and stopped. Updated packaged JARs were launched in hidden processes for the live test. At verification completion they remain running: producer Java PID **16352**, consumer Java PID **5180**. Stop these current application instances before launching equivalent IntelliJ run configurations; Kafka can remain running. Process IDs are observations and may change on restart.

Submitted:
```json
{"orderId":"ORD-9010","customerId":"CUST-101","product":"Laptop","quantity":2,"status":"CREATED"}
```

Actual response: **HTTP 202**
```json
{"orderId":"ORD-9010","status":"PUBLISHED","topic":"orders.created.v2","partition":0,"offset":0}
```

Producer log at 10:18:08.835 UTC:
```text
Kafka acknowledged orderId=ORD-9010 topic=orders.created.v2 partition=0 offset=0
```

Consumer log at 10:18:08.948 UTC:
```text
Order notification processed successfully: orderId=ORD-9010 customerId=CUST-101 product=Laptop quantity=2 status=CREATED
```

Consumer group `order-notification-v2-group`: current offset 1, log-end offset 1, **lag 0**.

Additional live HTTP checks:
- Blank order ID and quantity 0: HTTP 400 with field errors.
- Malformed JSON: HTTP 400 Problem Detail.

Evidence:
- `producer-service/target/demo-response.json`
- `producer-service/target/demo-producer.log`
- `consumer-service/target/demo-consumer.log`
- `producer-service/target/demo-processes.json`

The live request used HTTP; Swagger UI and the OpenAPI endpoint were checked by HTTP separately. No manual browser interaction is claimed. Broker-outage behavior and listener failure recovery were tested with mocks, not by disrupting the existing live broker.

## Source changes

Paths below are relative to the project root.

| File | Change |
| --- | --- |
| `producer-service/pom.xml` | Added Spring Boot validation starter; retained existing Boot/Kafka/Web/Swagger dependencies and versions. |
| `producer-service/src/main/java/com/henry/producerservice/event/OrderCreatedEvent.java` | Bean Validation constraints; nullable Integer quantity allows explicit rejection of missing quantity. |
| `producer-service/src/main/java/com/henry/producerservice/controller/OrderController.java` | Added @Valid; replaced manual checks with validation; asynchronous 202 response includes acknowledgment metadata. |
| `producer-service/src/main/java/com/henry/producerservice/service/OrderProducerService.java` | Preserved topic and keyed async send; added acknowledgment/failure logging and synchronous/asynchronous failure translation. |
| `producer-service/src/main/java/com/henry/producerservice/service/OrderPublicationException.java` | New publication failure type. |
| `producer-service/src/main/java/com/henry/producerservice/controller/GlobalExceptionHandler.java` | New global Problem Detail handler with field errors, 503/500 handling and standard MVC error responses. |
| `producer-service/src/main/resources/application.yaml` | All-replica acknowledgment, idempotence, bounded publication timeouts and MVC async timeout; retained Jackson 3 serializer. |
| `consumer-service/src/main/java/com/henry/consumer/ConsumerServiceApplication.java` | Restored correctly packaged @SpringBootApplication entry point; removed misplaced duplicate consumer declaration. |
| `consumer-service/src/main/java/com/henry/consumer/service/OrderConsumerService.java` | Uses configured v2 group; validates event values and logs all five fields on success. |
| `consumer-service/src/main/java/com/henry/consumer/config/KafkaConsumerConfiguration.java` | New bounded retry/recovery policy; logs failed record location and skips after recovery. |
| `consumer-service/src/main/resources/application.yaml` | ErrorHandlingDeserializer delegates to JacksonJsonDeserializer; record acknowledgment and auto-commit disabled. |
| Both existing `*ServiceApplicationTests.java` files | Disabled Kafka listener startup during context tests. |
| `producer-service/src/test/java/com/henry/producerservice/controller/OrderControllerTests.java` | New API success, validation and HTTP failure tests. |
| `producer-service/src/test/java/com/henry/producerservice/service/OrderProducerServiceTests.java` | New correct topic/key, acknowledgment and publishing failure tests. |
| `consumer-service/src/test/java/com/henry/consumer/service/OrderConsumerServiceTests.java` | New consumer logging and invalid-event tests. |
| `consumer-service/src/test/java/com/henry/consumer/config/KafkaConsumerConfigurationTests.java` | New Jackson 3 contract, malformed JSON, retry exhaustion and nonretryable recovery tests. |
| `.gitignore` | Root exclusions for build output, IDE files, logs, environment files and common secrets. |
| `README.md` | New assignment overview, architecture, setup, demonstrations, concepts, tests and failure-policy documentation. |
| `VERIFICATION.md` | This executed-results and change record. |

Reviewed and preserved: `compose.yaml`, consumer POM/event record, producer main entry point, Maven wrappers and existing service ignore files. Unrelated files were not removed.

## Limitations and next steps

- A single local broker with replication factor one is suitable for the assignment, not high availability.
- The consumer logs notifications; no email provider or database is implemented.
- Invalid/exhausted events are logged and skipped; there is no dead-letter topic or durable failed-event archive.
- At-least-once consumption can produce duplicates. Kafka producer idempotence does not deduplicate repeated HTTP requests.
- No Git repository exists at the root or inside either service. `git status` reports “not a git repository”; therefore there is no tracked diff/status to report. No repository initialization, commit, push or GitHub creation was performed.
- Review these files and run the documented demonstration. Initialize and commit the root project only when ready and authorized.
