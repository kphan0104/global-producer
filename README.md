# global-producer

`global-producer` is a Spring Boot 4 / Java 21 application that can:

- load flows from an external `data/` directory
- choose a random `.msg` template for each flow on every execution
- render variables and timestamp profiles
- publish a Databus JSON envelope to Kafka
- expose a REST API to publish an arbitrary `topic` / `key` / `value` payload to Kafka

## Requirements

- Java `21`
- Maven `3.9+`
- Kafka broker reachable by the app

Runtime configuration is intentionally external to the jar.

## Run

From the repository root:

```bash
mvn spring-boot:run
```

Or build the jar:

```bash
mvn clean package
java -jar target/global-producer-1.0.0-SNAPSHOT.jar
```

## External application.yml

The project no longer packages an `application.yml` inside the jar.

Provide your runtime configuration next to the jar, for example:

```text
global-producer-1.0.0-SNAPSHOT.jar
application.yml
data/
```

An example file is available in the repository at [application.yml.example](/Users/kph/Documents/global-producer/application.yml.example).

Spring Boot will also accept:

```text
config/application.yml
```

Typical startup:

```bash
java -jar global-producer-1.0.0-SNAPSHOT.jar
```

## External data directory

By default, the application reads flows from:

```text
./data
```

You can override it with:

```bash
java -jar target/global-producer-1.0.0-SNAPSHOT.jar --app.data-directory=/absolute/path/to/data
```

## Flow structure

Each flow lives in its own folder:

```text
data/
  flow1/
    flow1.yml
    titi.msg
    toto.msg
  flow2/
    flow2.yml
    mimi.msg
    mama.msg
    momo.msg
```

Rules:

- one folder = one flow
- the config file name must match the folder name
- supported config extensions: `.yml` and `.yaml`
- each flow must contain at least one `.msg`
- on every execution, the app randomly picks one `.msg` inside the flow

## Flow configuration

Examples:

```yaml
topic: demo.flow1

schedule:
  duration: 10s
  # or:
  # cron: "0 */1 * * * *"
  # timezone: Europe/Paris

timestamp:
  format: "yyyy-MM-dd HH:mm:ss.SSS"
  timezone: Europe/Paris
  locale: fr

variables:
  ENV:
    choice: ["DEV", "QA", "PROD"]

  EVENT_ID:
    pattern: "[A-Z]{3}[0-9]{5}"
```

Single-profile flows can use the direct `timestamp.format/timezone/locale` form above.

If a flow needs several timestamp formats, switch to named profiles:

```yaml
topic: demo.flow1

schedule:
  duration: 10s

timestamp:
  event_time:
    format: NOW
    locale: en

  producer_offset:
    format: ISO_OFFSET_DATE_TIME
    timezone: "+02:00"
    locale: en

  paris_sql:
    format: "yyyy-MM-dd HH:mm:ss.SSS"
    timezone: Europe/Paris
    locale: fr

variables:
  ENV:
    choice: ["DEV", "QA", "PROD"]

  EVENT_ID:
    pattern: "[A-Z]{3}[0-9]{5}"
```

## Schedule

Exactly one of these must be configured:

- `schedule.duration`
- `schedule.cron`

### Duration

Examples:

- `10s`
- `500ms`
- `1m`
- `2h`

### Cron

Example:

```yaml
schedule:
  cron: "0 */1 * * * *"
  timezone: Europe/Paris
```

Rules:

- `schedule.timezone` is used to evaluate the cron trigger
- if a flow uses `cron` and has multiple timestamp profiles, `schedule.timezone` is required
- if a flow uses `cron` and has exactly one timestamp profile with a `timezone`, the app can fall back to that profile timezone when `schedule.timezone` is omitted
- if the only timestamp profile uses `NOW`, you still need `schedule.timezone` for `cron`

## Timestamp profiles

`timestamp` supports two shapes:

- single-profile flows:
  ```yaml
  timestamp:
    format: "yyyy-MM-dd HH:mm:ss.SSS"
    timezone: Europe/Paris
    locale: fr
  ```
- multi-profile flows:
  ```yaml
  timestamp:
    event_time:
      format: NOW
      locale: en
    local_time:
      format: "yyyy-MM-dd HH:mm:ss.SSS"
      timezone: Europe/Paris
      locale: fr
  ```

If a flow defines a single timestamp profile, you can reference it directly inside `.msg` files with:

```text
${TIMESTAMP}
```

If a flow defines multiple timestamp profiles, reference them with:

```text
${TIMESTAMP:profile_name}
```

Examples:

```text
${TIMESTAMP}
${TIMESTAMP:event_time}
${TIMESTAMP:producer_offset}
${TIMESTAMP:paris_sql}
```

Rules:

- `${TIMESTAMP}` is allowed only when the flow defines exactly one timestamp profile
- if the flow defines multiple timestamp profiles, `${TIMESTAMP:profile_name}` is required
- only `TIMESTAMP` supports the `:profile_name` suffix
- variables like `${ENV}` and `${TRACE_ID}` keep their normal syntax
- the same `Instant` is reused for all timestamp placeholders rendered in the same message

### Supported timestamp formats

#### `NOW`

`NOW` renders the real current instant in UTC with `Z`.

Example:

```text
2026-05-15T21:41:32.866215Z
```

Config:

```yaml
timestamp:
  event_time:
    format: NOW
```

Notes:

- `timezone` is not required for `NOW`
- `NOW` always renders UTC

#### `ISO_OFFSET_DATE_TIME`

This renders a timestamp with an explicit offset.

Example:

```text
2026-05-15T23:41:32.866215+02:00
```

Config:

```yaml
timestamp:
  producer_offset:
    format: ISO_OFFSET_DATE_TIME
    timezone: "+02:00"
    locale: en
```

#### Custom Java date pattern

Examples:

- `EEE MMM d HH:mm:ss yyyy`
- `MMM d yyyy HH:mm:ss`
- `yyyy-MM-dd HH:mm:ss.SSS`
- `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`

Config:

```yaml
timestamp:
  syslog_time:
    format: "EEE MMM d HH:mm:ss yyyy"
    timezone: Europe/Paris
    locale: en
```

### Timezone values

Supported values include:

- `UTC`
- `Europe/Paris`
- `Africa/Algiers`
- `+02:00`
- `-05:00`

### Locale

`locale` is optional.

It is mainly useful when your format contains textual month/day fields like:

- `EEE`
- `MMM`

Examples:

- `en`
- `fr`
- `en-US`
- `fr-FR`

## Variables

Variables are rendered with the normal placeholder syntax:

```text
${ENV}
${TRACE_ID}
${EVENT_ID}
```

### `choice`

Example:

```yaml
variables:
  ENV:
    choice: ["DEV", "QA", "PROD"]
```

One value is chosen randomly for each message render.

### `pattern`

Example:

```yaml
variables:
  EVENT_ID:
    pattern: "[A-Z]{3}[0-9]{5}"
```

The pattern engine is a controlled subset, not a full regex engine.

Supported pattern features:

- character classes like `[A-Z]`, `[0-9]`, `[A-F0-9]`
- fixed repetition like `{3}`, `{5}`, `{16}`
- literal characters

Examples:

- `[A-Z]{3}[0-9]{5}`
- `[0-9]{10}`
- `[A-F0-9]{16}`

Not supported:

- groups `(...)`
- alternation `|`
- optional markers `?`
- wildcards `.`
- `+`
- `*`
- anchors `^` / `$`

## Example `.msg`

```json
{
  "flow": "flow1",
  "eventTime": "${TIMESTAMP}",
  "localParisTime": "${TIMESTAMP:paris_sql}",
  "environment": "${ENV}",
  "eventId": "${EVENT_ID}"
}
```

## Runtime behavior

For each loaded flow:

- the app schedules the flow using `duration` or `cron`
- at each execution, it picks one `.msg` randomly
- it renders timestamp profiles and variables
- it publishes the final payload as a Kafka `String`
- the Kafka record timestamp is set to the real current instant used for rendering

The app also watches the `data/` directory and reloads flows when `.msg`, `.yml`, or `.yaml` files change.

Before publishing to Kafka, each rendered flow message is wrapped in a JSON envelope with these fields:

```json
{
  "databus.flow.name": "flow1",
  "databus.flow.provider.name": "integration-tests",
  "originalMessage": "{\"eventTime\":\"2026-05-15T21:41:32.866215Z\"}",
  "databus.event.lineage.stage1.timestamp": "2026-05-15T21:41:32.866215Z",
  "databus.event.lineage.stage1.pipeline_id": "global_producer"
}
```

Field behavior:

- `databus.flow.name`: the flow folder name
- `databus.flow.provider.name`: configured from `app.databus.flow.provider.name`
- `originalMessage`: the fully rendered `.msg` content that used to be published directly
- `databus.event.lineage.stage1.timestamp`: the current production instant as an ISO-8601 UTC string
- `databus.event.lineage.stage1.pipeline_id`: fixed to `global_producer`

## REST API

The application exposes:

```text
POST /api/messages
```

Swagger / OpenAPI endpoints:

```text
GET /swagger-ui.html
GET /v3/api-docs
GET /v3/api-docs.yaml
```

Request body:

```json
{
  "topic": "demo.manual",
  "key": "customer-42",
  "value": "{\"status\":\"OK\"}"
}
```

Behavior:

- `topic` is the Kafka topic to publish to
- `key` is the Kafka key
- `value` is the Kafka payload sent as-is

Response example:

```json
{
  "topic": "demo.manual",
  "key": "customer-42",
  "value": "{\"status\":\"OK\"}",
  "partition": 0,
  "offset": 123,
  "timestamp": 1779134492866
}
```

## Validation rules

A flow is rejected if:

- the folder does not contain a matching `.yml` or `.yaml`
- the folder contains no `.msg`
- both `.yml` and `.yaml` exist at the same time
- both `schedule.duration` and `schedule.cron` are defined
- neither `schedule.duration` nor `schedule.cron` is defined
- `${TIMESTAMP}` is used without a profile name in a flow that defines multiple timestamp profiles
- a timestamp profile is referenced but not declared
- a variable placeholder is referenced but not declared
- a variable defines both `choice` and `pattern`
- a variable defines neither `choice` nor `pattern`

## Tests

Run the full suite with:

```bash
mvn verify
```

The test suite covers:

- unit tests for pattern generation, rendering, validation, scheduling, and Kafka publish service
- integration tests for flow-to-Kafka publication
- integration tests for the REST API with embedded Kafka
