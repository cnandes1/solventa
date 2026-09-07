# Contratos de eventos

Todos los eventos de negocio usan Redis Streams en `redis-business`. Los mensajes de salud usan Redis Pub/Sub en `redis-control`. Los timestamps están expresados en ISO 8601 UTC.

## ProfileRefreshRequested

- Productor: Quoting.
- Consumidor: Profiling.
- Stream: `profile-refresh-requests`.
- Propósito: solicitar un recálculo sin acoplamiento HTTP entre servicios.

```json
{
  "eventId": "uuid",
  "eventType": "ProfileRefreshRequested",
  "correlationId": "uuid",
  "customerId": "C001",
  "requestedAt": "2026-09-06T15:00:00+00:00",
  "requestedBy": "quoting-a"
}
```

## ProfileCalculationRequested

- Productor: orquestador interno de Profiling.
- Consumidores: estrategias A, B y C, cada una con consumer group independiente.
- Stream: `profile-calculation-requests`.
- Propósito: iniciar la votación correlacionada.

```json
{
  "eventId": "uuid",
  "eventType": "ProfileCalculationRequested",
  "correlationId": "uuid",
  "customerId": "C001",
  "timestamp": "2026-09-06T15:00:00+00:00"
}
```

## ProfilingResult

- Productores: estrategias A, B y C.
- Consumidor: Validator de Profiling.
- Stream: `profiling-results`.
- Propósito: entregar un voto sin mezclar cálculos concurrentes.

```json
{
  "eventId": "uuid",
  "eventType": "ProfilingResult",
  "correlationId": "uuid",
  "strategyId": "A",
  "customerId": "C001",
  "riskScore": "40.0",
  "status": "SUCCESS",
  "profileSource": "OPEN_FINANCE",
  "timestamp": "2026-09-06T15:00:00+00:00"
}
```

El Validator espera A/B/C hasta `VOTING_TIMEOUT_MS`. Dos votos dentro de `VOTING_TOLERANCE` forman consenso. Un valor fuera del grupo mayoritario queda registrado como outlier.

## ProfileUpdated

- Productor: Profiling después de un consenso válido.
- Consumidores: materializadores de Quoting A y B.
- Stream: `profile-updated`.
- Propósito: transferir estado durable a cada réplica.

```json
{
  "eventId": "uuid",
  "eventType": "ProfileUpdated",
  "correlationId": "uuid",
  "customerId": "C001",
  "version": "10",
  "riskScore": "40.0",
  "riskLevel": "MEDIUM",
  "timestamp": "2026-09-06T15:00:00+00:00"
}
```

Cada materializador procesa el evento en una transacción SQLite. Un `eventId` repetido produce `DUPLICATE`; una versión menor o igual produce `OLD_VERSION`; solo una versión mayor produce `APPLIED`.

## HealthPing

- Productor: Health Monitor del Gateway o monitor de dependencia de Quoting.
- Consumidor: instancia indicada por `targetInstanceId`.
- Canal: `health-heartbeat`.

```json
{
  "eventType": "HealthPing",
  "correlationId": "uuid",
  "targetInstanceId": "quoting-b",
  "timestamp": "2026-09-06T15:00:00+00:00",
  "requestedBy": "health-monitor"
}
```

## HealthEcho

- Productor: instancia consultada.
- Consumidor: monitor que originó el ping.
- Canal: `health-heartbeat`.

```json
{
  "eventType": "HealthEcho",
  "correlationId": "uuid",
  "instanceId": "quoting-b",
  "serviceName": "quoting",
  "timestamp": "2026-09-06T15:00:00+00:00",
  "status": "UP"
}
```
