# Solventa Availability Experiment

Experimento reproducible para evaluar si transferencia de estado, vistas materializadas locales y detección asíncrona de fallas sostienen el journey de Cotización bajo las condiciones definidas para ASR-02 y ASR-05.

Un segundo experimento, de seguridad, evalúa AS-4 (confidencialidad) y AS-8 (integridad) sobre el mismo montaje; ver [Experimento de seguridad](#experimento-de-seguridad-as-4--as-8).

El resultado de una ejecución representa evidencia dentro del entorno evaluado. No equivale a una predicción de disponibilidad mensual en producción.

## Hipótesis

- H1, transferencia de estado: un `ProfileUpdated` durable permite cotizar desde SQLite local cuando Profiling está temporalmente indisponible.
- H2, detección: Ping-Echo por el plano de control retira una réplica que no responde dentro de la ventana configurada.
- H3, reintegro: una réplica recuperada pasa por `DOWN -> SHADOW -> ACTIVE` antes de recibir tráfico autoritativo.
- H4, votación: A/B/C publican resultados correlacionados y el Validator decide por consenso o timeout sin mezclar ejecuciones.

## Arquitectura

```mermaid
flowchart LR
    OF[Open Finance Mock]
    P[Profiling]
    RB[(Redis Business + AOF)]
    RC[(Redis Control)]
    QA[Quoting A]
    QB[Quoting B]
    DBA[(SQLite A)]
    DBB[(SQLite B)]
    G[Gateway + Health Monitor]

    P -->|HTTP 700 ms + Circuit Breaker| OF
    QA -->|ProfileRefreshRequested| RB
    QB -->|ProfileRefreshRequested| RB
    RB -->|ProfileCalculationRequested| P
    P -->|ProfilingResult A/B/C| RB
    P -->|ProfileUpdated| RB
    RB -->|quoting-a-materializer| QA
    RB -->|quoting-b-materializer| QB
    QA --> DBA
    QB --> DBB
    G -->|solo ACTIVE| QA
    G -->|solo ACTIVE| QB
    RC <-->|HealthPing / HealthEcho| G
    RC <-->|HealthPing / HealthEcho| QA
    RC <-->|HealthPing / HealthEcho| QB
```

### Componentes

| Componente | Responsabilidad | Estado |
|---|---|---|
| `redis-business` | Streams de negocio con AOF y volumen persistente | Durable dentro del montaje local |
| `redis-control` | Pub/Sub efímero de HealthPing/HealthEcho | Separado del plano de negocio |
| `open-finance-mock` | Modos determinísticos NORMAL, SLOW, HTTP_500, TIMEOUT y DOWN | Inyección controlada |
| `profiling` | Circuit Breaker, caché, estrategias A/B/C, Validator y `ProfileUpdated` | Command + Query experimental |
| `quoting-a/b` | Materializador independiente y journey EDA desde SQLite | Read model local por réplica |
| `gateway` | Routing ACTIVE, failover único, retiro y SHADOW | Health Monitor incluido |

## Decisiones verificables

### Vista local e idempotencia

Cada réplica monta un volumen diferente y configura su propio `SQLITE_PATH`. La tabla `profile_view` nunca se consulta en Redis durante una cotización. La aplicación de eventos usa una transacción `BEGIN IMMEDIATE`, registra `eventId` y solo actualiza cuando la versión entrante es mayor.

Un perfil inexistente o vencido produce `503` con `NO_VALID_PROFILE_AVAILABLE` o `PROFILE_EXPIRED`. `MAX_PROFILE_AGE_SECONDS=300` es un umbral experimental configurable, no un requisito oficial.

### Consumer groups

`quoting-a-materializer` y `quoting-b-materializer` son grupos diferentes. Ambos reciben cada `ProfileUpdated`. Los pendientes se reclaman con `XAUTOCLAIM` después de `PENDING_CLAIM_IDLE_MS`; nunca se utiliza `min_idle_time=0`.

### Journey EDA y baseline síncrono

- `GET /quotes/{customerId}` consulta SQLite local.
- `POST /quotes/{customerId}/request-refresh` publica `ProfileRefreshRequested`.
- `GET /sync/quotes/{customerId}` llama a Profiling y existe únicamente para demostrar la propagación de fallas de la arquitectura anterior.

### Circuit Breaker y fallback

Profiling limita Open Finance mediante `OPEN_FINANCE_TIMEOUT_MS`. Después de `CB_FAILURE_THRESHOLD`, el circuito pasa a OPEN. Al finalizar `CB_RECOVERY_TIMEOUT_SECONDS`, permite una sola prueba HALF_OPEN. Si existe caché vigente, estrategia A usa `CACHE`; si no existe, A reporta error y la política 2/3 todavía puede decidir con B/C.

### Votación por eventos

Cada cálculo publica `ProfileCalculationRequested`. A, B y C consumen mediante grupos independientes y publican `ProfilingResult`. El Validator agrupa por `correlationId`, espera hasta `VOTING_TIMEOUT_MS` y busca el grupo mayoritario dentro de `VOTING_TOLERANCE`.

Para `40, 40, 90`, el resultado es `40` y C queda como outlier. Para `40, 40, timeout`, A/B forman consenso y C aparece en `missingStrategies`.

### Health, failover y SHADOW

El Gateway envía HealthPing por Redis Control. Dos ausencias consecutivas, con la configuración incluida, cambian la réplica a DOWN. El routing utiliza exclusivamente instancias ACTIVE.

Si una instancia falla antes de quedar DOWN, el Gateway hace un solo reintento ante error de transporte contra otra instancia ACTIVE. Una réplica recuperada entra en SHADOW. Los GET autoritativos se comparan con respuestas shadow; la promoción requiere health checks suficientes, tiempo mínimo, validaciones y cero diferencias.

El Docker healthcheck comprueba infraestructura del experimento. Ping-Echo es la táctica arquitectónica bajo evaluación; cumplen objetivos diferentes.

## Ejecución

### Requisitos

- Docker Desktop con Docker Compose.
- Python 3.10 o superior para scripts y pruebas locales.

```bash
git checkout feature/mejoras-experimento
docker compose up --build -d --wait
docker compose ps
```

Puertos host: Gateway `8080`, Profiling `7500`, Quoting A `7002`, Quoting B `7001`, Open Finance `6000`, IdP de prueba `6100`, PDP `6200`, Redis Business `6379` y Redis Control `6380`. Quoting A usa `7002` porque macOS suele reservar `7000` para Control Center; dentro de Docker ambas réplicas escuchan en `7000`.

Comprobación inicial:

```bash
curl -s -X POST http://localhost:7500/profiles/C001/refresh | python -m json.tool
curl -s http://localhost:7002/materialized-profiles/C001 | python -m json.tool
curl -s http://localhost:7001/materialized-profiles/C001 | python -m json.tool
TOKEN=$(curl -s -X POST http://localhost:6100/tokens -H 'Content-Type: application/json' \
  -d '{"sub":"C001","scopes":["quotes:read"]}' | python -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
curl -s http://localhost:8080/quotes/C001 -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

Desde el experimento de seguridad, el Gateway exige un JWT para `/quotes/*`. El runner y los scripts de carga obtienen un token del IdP de prueba para cada cliente.

## Pruebas automatizadas

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -v
```

Con el stack levantado:

```bash
RUN_INTEGRATION=1 pytest -m integration -v
```

Las pruebas unitarias cubren Circuit Breaker, versionado/idempotencia, consenso y la máquina `ACTIVE/DOWN/SHADOW`. Las pruebas de arquitectura verifican separación de buses, grupos y volúmenes independientes, ausencia de HTTP en el refresh EDA y failover acotado.

## Runner E0-E9

```bash
python scripts/run_experiment.py E0
python scripts/run_experiment.py E3
python scripts/run_experiment.py E5
python scripts/run_experiment.py E6
python scripts/run_experiment.py E7
python scripts/run_experiment.py E8
python scripts/run_experiment.py E9
python scripts/run_experiment.py all
```

Cada ejecución guarda `results/E<n>.json` y actualiza `results/summary.csv` y `results/acceptance_matrix.csv`. Un resultado PASS significa que el comportamiento observado cumplió el criterio programado para esa ejecución.

| Escenario | Falla o condición | Evidencia principal |
|---|---|---|
| E0 | Todo disponible | Disponibilidad, throughput, p50/p95/p99 |
| E1 | Open Finance DOWN | Fallback, circuito y continuidad |
| E2 | Open Finance lento | Timeout y contención de latencia |
| E3 | Profiling DOWN | EDA disponible y baseline síncrono fallando |
| E4 | Materializador A pausado | Pendientes reclamados y versiones A/B iguales |
| E5 | Quoting B DOWN | B retirado, failover y continuidad por A |
| E6 | Quoting B recuperado | Secuencia DOWN-SHADOW-ACTIVE |
| E7 | A=40, B=40, C=90 | C marcado como outlier |
| E8 | C no responde | Decisión A/B después del timeout |
| E9 | Redis Business DOWN | Cotización desde SQLite y salud por Redis Control |

## Experimento de seguridad (AS-4 / AS-8)

Plan completo: [docs/security-experiment-plan.md](docs/security-experiment-plan.md). Guion de video: [docs/video-demo-security.md](docs/video-demo-security.md).

- **H-AS4, confidencialidad.** El Gateway valida el JWT (firma HS256, `exp`, `aud`, `iss`, `kid`) y consulta al PDP con `{subject, resource, action}`. El PDP deniega si el tenant no coincide, si falta el scope de la acción o si el sujeto no es el dueño y no tiene `delegated:<owner>`. Si el PDP no responde en `PDP_TIMEOUT_MS`, la respuesta es `503 PDP_UNAVAILABLE`: el sistema falla cerrado siempre. Las rutas no mapeadas se deniegan. `/gateway/status`, `/metrics`, `/health` y `/admin/*` quedan fuera de esta capa.
- **H-AS8, integridad.** Profiling firma cada `ProfileUpdated` con HMAC-SHA256. Quoting verifica la firma antes de materializar el evento y descarta los eventos alterados, sin firma o con un `keyId` desconocido. La ACL de Redis Business restringe quién puede publicar y consumir en `profile-updated` ([redis/README.md](redis/README.md)).
- **Auditoría.** Los eventos `AUTHZ_DECISION` (Gateway) e `INTEGRITY_CHECK` (Quoting) usan el mismo formato JSON que los demás logs. Nunca incluyen el JWT ni secretos HMAC.

Todas las llaves y contraseñas son valores `TEST-ONLY-*` que se definen en `docker-compose.yaml`.

```bash
python scripts/run_experiment.py SEC-C3
python scripts/run_experiment.py all-security      # SEC-C0..C9 y SEC-I0..I9
python scripts/run_experiment.py all-experiments   # E0..E9 y después SEC-*
RUN_INTEGRATION=1 pytest -m integration tests/security -v
```

`all` sigue ejecutando solo `E0`-`E9`. Los resultados quedan en `results/SEC-*.json` y se agregan como filas a `summary.csv` y `acceptance_matrix.csv`, con las mismas columnas.

| Escenario | Condición | Criterio programado |
|---|---|---|
| SEC-C0 | Dueño con `quotes:read` | 200 en todas las llamadas |
| SEC-C1 | Sin `Authorization` | 401 `MISSING_TOKEN` |
| SEC-C2 | Token vencido | 401 `TOKEN_EXPIRED` |
| SEC-C3 | Token de C002 pide C001 | 403 `OWNERSHIP_MISMATCH` |
| SEC-C4 | Solo `profiles:read` | 403 `INSUFFICIENT_SCOPE` |
| SEC-C5 | C002 con `quotes:read` + `delegated:C001` | 200 y auditoría `PERMIT/DELEGATION` |
| SEC-C6 | Tenant del token distinto | 403 `TENANT_MISMATCH` |
| SEC-C7 | `docker compose stop pdp` | 503 `PDP_UNAVAILABLE`, nunca 200 |
| SEC-C8 | Un carácter de la firma alterado | 401 `INVALID_SIGNATURE` |
| SEC-C9 | Carga de 6 s con autorización | 100% y overhead medio de autorización reportado |
| SEC-I0 | Refresh normal | Verificado y `APPLIED` en A/B |
| SEC-I1 | `riskScore` alterado conservando `payloadHash` | `SIGNATURE_MISMATCH`, vista y prima sin cambios |
| SEC-I2 | Evento sin campos de integridad | `MISSING_INTEGRITY_FIELDS` |
| SEC-I3 | Firmado con `keyId` desconocido | `UNKNOWN_KEY` |
| SEC-I4 | `XADD` anónimo o con rol consumidor | `NOPERM` y nada consumido |
| SEC-I5 | `XREADGROUP`/`XRANGE` anónimo o con rol productor | `NOPERM` |
| SEC-I6 | Rotación a `test-key-2026-10` | Llave nueva y anterior verifican, 0 rechazos |
| SEC-I7 | Reenvío del mismo evento firmado | `DUPLICATE`, 0 rechazos |
| SEC-I8 | Carga + refrescos continuos | 100%, todos verificados, 0 rechazos |
| SEC-I9 | Rechazos I1+I2+I3 | 3 razones auditadas y 0 secretos en logs |

Decisiones de implementación que conviene conocer:

- Para que la idempotencia o el versionado no oculten la falta de verificación, SEC-I1, SEC-I2 y SEC-I3 publican el evento falsificado con un `eventId` nuevo y una `version` mayor. Sin verificación, ese evento se aplicaría.
- Los eventos falsificados se inyectan con las credenciales `profile_producer`. Esto simula un atacante que obtuvo acceso al bus pero no la llave HMAC, y muestra que las dos defensas son independientes.
- Una delegación no reemplaza el scope de la acción: SEC-C5 usa `["quotes:read", "delegated:C001"]`.
- El Gateway valida el JWT localmente con la llave compartida del IdP (`IDP_KEYS_JSON`, por `kid`). El IdP solo emite tokens y no participa en cada request.
- SEC-C9 mide el costo de autorización con `authz_duration_us_total / authz_checks` del Gateway. SEC-I8 compara su p95 y throughput con `results/E0.json`.

## Carga con Locust

```bash
python -m venv .venv-locust
source .venv-locust/bin/activate
pip install -r load-tests/requirements.txt
TARGET_HOST=http://localhost:8080 CUSTOMER_POOL=C001,C002,C003 \
  locust -f load-tests/locustfile.py --users 20 --spawn-rate 5
```

Abre `http://localhost:8089`. Precarga los clientes con E0 antes de iniciar carga; los clientes sin perfil válido deben fallar funcionalmente y no se contabilizan como disponibilidad exitosa.

## Endpoints de control del experimento

Open Finance lento:

```bash
curl -s -X POST http://localhost:6000/admin/mode \
  -H 'Content-Type: application/json' \
  -d '{"mode":"SLOW","latencyMs":1500}'
```

Votación discrepante:

```bash
curl -s -X POST http://localhost:7500/admin/voting-scenario \
  -H 'Content-Type: application/json' \
  -d '{"A":{"mode":"NORMAL","score":40},"B":{"mode":"NORMAL","score":40},"C":{"mode":"NORMAL","score":90}}'
```

Estado del Gateway y métricas:

```bash
curl -s http://localhost:8080/gateway/status | python -m json.tool
curl -s http://localhost:7500/metrics | python -m json.tool
curl -s http://localhost:7002/metrics | python -m json.tool
```

Los endpoints `/admin/*` son exclusivos del mock y del entorno experimental.

## Criterios de aceptación

| Hipótesis | Escenario | Métrica | Criterio programado |
|---|---|---|---|
| H1 | E3 | availability | 100% para perfiles precargados; baseline sync falla |
| H1 | E4 | pending_recovered | Al menos un pendiente y misma versión final en A/B |
| H1 | E9 | availability | 100% para perfiles vigentes en SQLite |
| H2 | E5 | estado y disponibilidad | B=DOWN y 100% de cotizaciones funcionales |
| H3 | E6 | transiciones | SHADOW observado antes de ACTIVE |
| H4 | E7 | outlier | finalScore=40, outlierStrategies=[C] |
| H4 | E8 | timeout | finalScore=40, missingStrategies=[C], duración <2.5 s |
| Equipo | E0/E4 | propagación | p95 ≤2 s, umbral experimental |

## Logs y contratos

Los servicios imprimen JSON estructurado con timestamp, servicio, instancia, evento, correlación y resultado cuando aplica.

- Contratos: [docs/events.md](docs/events.md)
- Guion de video: [docs/video-demo.md](docs/video-demo.md)
- Guion de video de seguridad: [docs/video-demo-security.md](docs/video-demo-security.md)

## Limitaciones

- Redis Business usa AOF y un volumen local. Esto no demuestra durabilidad multi-región ni alta disponibilidad de un clúster Redis.
- Redis Control usa Pub/Sub efímero; perder mensajes de health forma parte de la semántica de detección.
- Las métricas residen en memoria y se reinician con cada contenedor.
- El runner genera evidencia en una sola máquina y durante intervalos cortos.
- Flask se ejecuta con su servidor integrado porque el objetivo es un experimento local, no un despliegue productivo.
- El IdP y el PDP son de prueba: HS256 con llave compartida y sin JWKS, revocación ni mTLS entre servicios. `redis-control` queda sin ACL, fuera del alcance de AS-8.
- SQLite representa una vista local por réplica. El experimento no evalúa replicación de la base ni crecimiento masivo.

## Limpieza

```bash
docker compose down
```

Para eliminar también AOF, vistas SQLite y resultados de estado persistente de Docker:

```bash
docker compose down -v
```
