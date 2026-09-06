# Experimento de disponibilidad EDA — Solventa

El diseno del experimento 1 valida: 
**ASR-02** (Profiling tolerante a fallas del proveedor externo) 
**ASR-05** (alta disponibilidad del journey de Quoting) 
mediante transferencia de estado asincrona (Redis Streams), 
Ping-Echo asincrono (Redis Pub/Sub) y votacion correlacionada (estrategias A/B/C).

## Topologia

| Servicio | Rol | Puerto host |
|---|---|---|
| `redis` | Bus de negocio (Streams: `profile-updated`, `profiling-vote-results`) y bus de control/salud (Pub/Sub: `health-heartbeat`), en el mismo Redis pero con canales/estructuras separadas. | 6379 |
| `risk-provider` | Mock de Open Finance/Open Data. `LATENCY_MS`/`FAIL` controlan la inyeccion de fallas (E1/E2). | 6000 |
| `profiling` | 3C/3Q. Circuit breaker + timeout 700ms hacia `risk-provider`, Profile Cache, votacion A/B/C por `correlationId`, publica `ProfileUpdated`. | 7500 |
| `quoting-a`, `quoting-b` | 4C/4Q. Consumen `ProfileUpdated` (consumer group `quoting-cg`), mantienen la vista materializada de perfil por `customer_id`. Ya no llaman sincronamente a nadie. | 7000, 7001 |
| `gateway` | Monitorea el pool de Quoting (`quoting-a`/`quoting-b`) via Ping-Echo asincrono, retiro/reintegro (E5/E6), proxy de trafico real. | 8080 |

## 0. Levantar el stack

```bash
cd solventa
docker compose up --build --wait
docker compose ps
```

## E0 — Linea base

Publicar un `ProfileUpdated` y confirmar que se propaga a la
vista materializada de perfil de Quoting en pocos segundos:

```bash
python3 experiment/measure_profile_updated.py propagation http://localhost:7500 http://localhost:7000 1
```

**Que observar:** `PASS` con latencia de propagacion baja (umbral
experimental sugerido: p95 <= 2s).

Generar carga base contra el pool de Quoting (a traves del gateway):

```bash
python3 experiment/measure_availability.py load http://localhost:8080 60
```

## E1 — Open Finance (risk-provider) DOWN

```bash
docker compose stop risk-provider
docker compose run -e FAIL=true -e LATENCY_MS=50 --rm -p 6000:6000 --name risk-provider-down risk-provider
# o editar `environment: FAIL: "true"` y `docker compose up -d --build risk-provider`
python3 experiment/measure_profiling.py degraded http://localhost:7500 20 1
```

**Que observar:** 100% de `/profiles/<id>/refresh` responden 200 (la falla
nunca llega al llamador); el circuito pasa a `open` tras
`CB_FAILS_THRESHOLD` fallos consecutivos (default 3); el score final sigue
saliendo de Profile Cache / estrategias B-C. Quoting sigue sirviendo su
vista materializada sin llamar a nadie (0 GET sincronos).

## E2 — Open Finance lento (>700ms)

Igual que E1 pero con `LATENCY_MS=2000` en vez de `FAIL=true`. El timeout
de 700ms de Profiling debe disparar antes de que el proveedor responda; el
resto del comportamiento (circuito, fallback) es identico.

## E3 — Profiling DOWN

```bash
docker compose stop profiling
python3 experiment/measure_availability.py load http://localhost:8080 20
```

**Que observar:** Quoting sigue respondiendo 200 con
`source=materialized-view` (o `default` si nunca hubo un evento previo para
ese cliente) usando el ultimo `ProfileUpdated` recibido -- cero GET
sincronos hacia Profiling (ya no existen en el codigo).

```bash
docker compose start profiling
```

## E4 — Consumidor de perfil (Quoting) DOWN

```bash
docker compose stop quoting-a
python3 experiment/measure_profile_updated.py order http://localhost:7500 http://localhost:7001 1 5
# publica 5 refrescos mientras quoting-a esta caido (usa quoting-b como testigo)
docker compose start quoting-a
python3 experiment/measure_profile_updated.py propagation http://localhost:7500 http://localhost:7000 1
```

**Que observar:** al reiniciar, `quoting-a` reclama pendientes
(`XPENDING`/`XCLAIM`, ver `quoting/app.py::_claim_pending`) y converge a la
misma version que `quoting-b` -- recuperacion completa, perdida cero.

## E5 — Instancia Quoting B DOWN

```bash
python3 experiment/measure_availability.py monitor http://localhost:8080 2
# en otra terminal:
docker compose stop quoting-b
```

**Que observar:** `quoting-b` pasa de `active` a `down` en <=2 ciclos de
Ping-Echo (ausencia de `HealthEcho` sobre `health-heartbeat`); `quoting-a`
sigue `active` y el proxy (`GET/POST` via `http://localhost:8080/...`) no
muestra errores nuevos.

## E6 — Reintegro de B

```bash
docker compose start quoting-b
```

**Que observar:** `quoting-b` pasa `down` -> `shadow` -> `active` (tras
`SHADOW_CYCLES` ciclos sanos); durante `shadow` es monitoreada pero no
recibe trafico real autoritativo del proxy.

## E7 — Votacion discrepante

Forzar una discrepancia manualmente: refrescar el perfil de un cliente
nuevo (cache fria) y observar que, ocasionalmente, la variacion
determinista entre estrategias supera el umbral
(`VOTE_DISCREPANCY_THRESHOLD`, default 15 puntos):

```bash
python3 experiment/measure_profiling.py normal http://localhost:7500 10 new-customer-1
```

**Que observar:** en la respuesta de `/refresh`, `vote.discrepancy` en
`true` para alguna ronda con cache fria; el `final_score` sigue siendo un
consenso (promedio) sin bloquear la respuesta.

## E8 — Votacion con ausencia

Bajar temporalmente `profiling` a un estado donde una estrategia
tarde/falle (p.ej. combinar con E1/E2, ya que la estrategia A depende del
circuit breaker) y confirmar via `vote.vote_incomplete=true` que el
Validador/Agregador decidio con los votos disponibles sin esperar
indefinidamente (timeout `VOTE_TIMEOUT_SECONDS`, default 1s).

## E9 — Bus de negocio (Redis) temporalmente DOWN

```bash
docker compose stop redis
python3 experiment/measure_availability.py load http://localhost:8080 10
docker compose start redis
python3 experiment/measure_profile_updated.py propagation http://localhost:7500 http://localhost:7000 1
```

**Que observar:** mientras `redis` esta caido, Quoting sigue sirviendo su
ultima vista materializada en memoria/Redis local (si Redis vuelve, sigue
disponible; si el propio Redis del stack cae, Quoting responde con el
ultimo dato leido antes de la caida y falla solo al intentar leer si Redis
no responde documentar el comportamiento observado). Al restaurar
`redis`, el consumidor retoma desde el consumer group sin perder eventos ya
confirmados.

## Limpieza

```bash
docker compose down -v
```

## Resumen de mapeo escenario

| # | Script | Criterio principal |
|---|---|---|
| E0 | `measure_profile_updated.py propagation` + `measure_availability.py load` | Propagacion rapida, disponibilidad 100% |
| E1/E2 | `measure_profiling.py degraded` | 0 errores propagados, circuito abre, fallback activo |
| E3 | `measure_availability.py load` (con `profiling` detenido) | 0 GET sincronos, vista materializada sirve |
| E4 | `measure_profile_updated.py order` | Recuperacion de pendientes, perdida cero |
| E5 | `measure_availability.py monitor` (pool quoting) | Retiro en <=2 ciclos, sin impacto en trafico |
| E6 | `measure_availability.py monitor` (pool quoting) | Reintegro via SHADOW, sin trafico autoritativo prematuro |
| E7 | `measure_profiling.py normal` (cliente nuevo) | Discrepancia detectada, consenso sin bloqueo |
| E8 | `measure_profiling.py` + inyeccion de falla en una estrategia | `vote_incomplete`, decision sin bloqueo indefinido |
| E9 | `measure_availability.py load` + `measure_profile_updated.py propagation` | Continuidad con ultimo dato, recuperacion sin corrupcion |
