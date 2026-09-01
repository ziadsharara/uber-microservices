# Uber-Style Ride-Hailing Microservices

A backend system that models the core ride-matching flow of an app like Uber, built as three independent Spring Boot microservices that communicate asynchronously over Kafka. It demonstrates event-driven architecture, geospatial queries with Redis, inter-service REST calls with Feign, and a ride lifecycle backed by MySQL.

> Side/portfolio project focused on the *matching pipeline* — how a ride request turns into a driver assignment through events, not on a full production Uber clone (no auth, payments, or real-time client push are included).

## Architecture

```
                         ┌────────────────┐
   POST /rides/request   │  ride-service  │
   ──────────────────▶  │ (MySQL, :8083) │
                         └────────┬───────┘
                                  │ 1. save Ride (REQUESTED)
                                  │ 2. publish RideRequestEvent
                                  ▼
                         ┌─────────────────┐        Kafka topic
                         │  ride.requested  │◀──────────────────────┐
                         └────────┬─────────┘                        │
                                  │ consumed by                      │
                                  ▼                                  │
                         ┌──────────────────┐   GET /drivers/nearby  │
                         │ matching-service  │──────────────────▶   │
                         │      (:8084)      │   ┌─────────────────┐ │
                         │  score & pick     │◀─│ location-service│ │
                         │  best driver      │   │  (Redis, :8082) │ │
                         └────────┬──────────┘   └─────────────────┘ │
                                  │ publish RideMatchedEvent         │
                                  ▼                                  │
                         ┌─────────────────┐                         │
                         │  ride.matched    │────────────────────────
                         └────────┬─────────┘
                                  │ consumed by ride-service
                                  ▼
                    Ride status: REQUESTED → MATCHING → ACCEPTED
```

Three services, each owning its own data store and responsibility:

| Service | Port | Owns | Talks to |
|---|---|---|---|
| **ride-service** | `8083` | Ride lifecycle & fare (MySQL) | Publishes to `ride.requested`, consumes `ride.matched` |
| **matching-service** | `8084` | Matching/scoring algorithm (stateless) | Consumes `ride.requested`, calls `location-service` (REST/Feign), publishes `ride.matched` |
| **location-service** | `8082` | Live driver GPS positions (Redis Geo) | Called by `matching-service` |

Kafka + Zookeeper act as the event backbone between `ride-service` and `matching-service`; the two never call each other directly — they only know about events. `location-service` is the one synchronous call in the pipeline, since a matching decision needs a live read of driver positions at the moment of matching.

## End-to-end flow

**1. Rider requests a ride**
`POST /api/rides/request` on `ride-service` → a `Ride` row is persisted with status `REQUESTED` → an estimated fare is computed (Haversine distance between pickup/drop × ₹12/km + ₹50 base) → a `RideRequestEvent` is published to the `ride.requested` Kafka topic → status flips to `MATCHING`.

**2. Matching service finds a driver**
`matching-service` consumes `ride.requested` → calls `location-service`'s `GET /locations/drivers/nearby` (via a Feign client) with the pickup coordinates and a 5 km radius → scores every candidate with a weighted formula (`70%` proximity, `30%` a simulated driver rating) → picks the highest-scoring driver → publishes a `RideMatchedEvent` to `ride.matched`.

**3. Ride is updated with the assigned driver**
`ride-service` consumes `ride.matched` → looks up the ride by ID → sets `driverId` and flips status to `ACCEPTED`.

**4. Driver location tracking (independent stream)**
Every ~3 seconds, a driver's client calls `POST /api/locations/drivers/update` on `location-service`, which upserts the driver's `(lon, lat)` into a single Redis geospatial index (`GEOADD`) keyed `drivers:locations`. Nearby-driver lookups use `GEORADIUS`/`geo().radius(...)` sorted ascending by distance. When a driver goes offline, `DELETE /api/locations/drivers/{driverId}` removes them from the index (`ZREM` under the hood).

**5. Ride lifecycle continues via REST**
Once matched, the ride progresses through plain REST calls on `ride-service`: `PUT /{rideId}/start` → `RIDE_STARTED`, `PUT /{rideId}/complete` → `COMPLETED` (actual fare = estimated fare), or `PUT /{rideId}/cancel` → `CANCELLED` at any point.

```
REQUESTED → MATCHING → ACCEPTED → DRIVER_ARRIVING → RIDE_STARTED → COMPLETED
                                        └──────────────► CANCELLED (at any stage)
```

## Kafka topics

| Topic | Producer | Consumer | Payload |
|---|---|---|---|
| `ride.requested` | ride-service | matching-service | `rideId`, `riderId`, pickup/drop lat-lng + address |
| `ride.matched` | matching-service | ride-service | `rideId`, `riderId`, `driverId`, driver lat-lng, distance to pickup |

Both topics are created with 3 partitions / 1 replica via a `KafkaConfig` bean in `ride-service`, and both services serialize events as JSON (`JsonSerializer`/`JsonDeserializer`).

## Driver matching algorithm

`matching-service` ranks each candidate returned by `location-service` with:

```
score = (1 / (distanceKm + 0.1)) * 0.7 + rating * 0.3
```

Distance dominates the score (closer drivers win), with a simulated `4.0–5.0` rating as a placeholder for a real driver-rating service. The highest-scoring driver is assigned. This is intentionally simple — it exists to show the *shape* of a matching pipeline (fetch candidates → score → decide → emit event), not a production-grade dispatch algorithm.

## Tech stack

- **Java 17**, **Spring Boot 4.1**
- **Spring Web MVC** — REST controllers
- **Spring Data Redis** — Redis Geo commands (`GEOADD`, `GEORADIUS`, `GEODIST`, `GEOPOS`) for driver locations
- **Spring Data JPA + MySQL** — ride persistence
- **Spring Kafka** — event publish/consume between ride-service and matching-service
- **Spring Cloud OpenFeign** — declarative REST client from matching-service to location-service
- **Lombok** — boilerplate reduction (`@Data`, `@RequiredArgsConstructor`, etc.)
- **Docker Compose** — Redis, MySQL, Zookeeper, Kafka for local development

## Running locally

### 1. Start infrastructure

```bash
docker-compose up -d
```

This brings up:
- **Redis** — `localhost:6379` (driver locations)
- **MySQL** — `localhost:3306`, db `ride_db`, `root/root` (ride data)
- **Zookeeper** — `localhost:2181`
- **Kafka** — `localhost:9092` (event streaming)

### 2. Run each service

Each service is independently runnable via its Maven wrapper:

```bash
cd location-service && ./mvnw spring-boot:run   # :8082
cd matching-service  && ./mvnw spring-boot:run   # :8084
cd ride-service      && ./mvnw spring-boot:run   # :8083
```

Database and Kafka connection details are configurable via environment variables (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`), with sane localhost defaults for local dev.

### 3. Try the flow

```bash
# Seed a driver's location near the pickup point
curl -X POST http://localhost:8082/api/locations/drivers/update \
  -H "Content-Type: application/json" \
  -d '{"driverId":"driver:1","latitude":12.9716,"longitude":77.5946}'

# Request a ride
curl -X POST http://localhost:8083/api/rides/request \
  -H "Content-Type: application/json" \
  -d '{
        "riderId":"rider:1",
        "pickupLatitude":12.9716,"pickupLongitude":77.5946,"pickupAddress":"MG Road",
        "dropLatitude":12.9352,"dropLongitude":77.6146,"dropAddress":"HSR Layout"
      }'

# Poll the ride — status should move from MATCHING to ACCEPTED once matching-service processes the event
curl http://localhost:8083/api/rides/{rideId}
```

You can also inspect the Redis geo index directly:

```bash
redis-cli
GEOADD drivers:locations 77.5946 12.9716 "driver:1"
GEOSEARCH drivers:locations FROMMEMBER "driver:1" BYRADIUS 5 km ASC COUNT 3
GEODIST drivers:locations "driver:1" "driver:2" km
GEOPOS drivers:locations "driver:1"
```

## API reference

### ride-service (`:8083/api`)

| Method | Path | Description |
|---|---|---|
| `POST` | `/rides/request` | Rider requests a ride; persists it and publishes `ride.requested` |
| `GET` | `/rides/{rideId}` | Fetch a ride by ID |
| `GET` | `/rides/rider/{riderId}` | List a rider's rides, newest first |
| `PUT` | `/rides/{rideId}/start` | Driver starts the ride (`ACCEPTED` → `RIDE_STARTED`) |
| `PUT` | `/rides/{rideId}/complete` | Completes the ride (`RIDE_STARTED` → `COMPLETED`) |
| `PUT` | `/rides/{rideId}/cancel` | Cancels the ride |

### location-service (`:8082/api`)

| Method | Path | Description |
|---|---|---|
| `POST` | `/locations/drivers/update` | Upserts a driver's lat/lng (called every ~3s by the driver client) |
| `GET` | `/locations/drivers/nearby?latitude=&longitude=&radius=` | Returns nearby drivers sorted by distance |
| `DELETE` | `/locations/drivers/{driverId}` | Removes a driver from the geo index (offline) |

### matching-service (`:8084`)

Has no public API of its own — it is purely event- and internal-call-driven (Kafka in, Feign out, Kafka out).

## Project structure

```
.
├── docker-compose.yml        # Redis, MySQL, Zookeeper, Kafka
├── location-service/          # Driver GPS tracking (Redis Geo)
├── matching-service/          # Driver matching/scoring (Kafka + Feign)
└── ride-service/               # Ride lifecycle & fare (MySQL + Kafka)
```

## Possible next steps

- Replace the simulated driver rating in `matching-service` with a real driver-profile lookup
- Add a WebSocket/SSE channel so riders get live status/driver-location push instead of polling
- Add a dead-letter topic for failed `ride.requested` processing (currently just logged and dropped)
- Add an API gateway and service discovery (Eureka/Consul) in front of the three services
- Containerize each service with its own Dockerfile and add them to `docker-compose.yml`
