# Plan de implementación — Experimento de seguridad AS-4 / AS-8

Este documento extiende el experimento de disponibilidad ya existente (`README.md`, escenarios `E0`-`E9`) con un segundo experimento, centrado en las historias de seguridad **AS-4 [Confidencialidad]** y **AS-8 [Integridad]**, tal como quedaron especificadas en `Experimento.pptx`. El objetivo es el mismo método científico: hipótesis, montaje reproducible, escenarios ejecutables con `docker compose`, evidencia en `results/*.json` y un guion de video. Ningún cambio descrito aquí modifica el comportamiento que valida `E0`-`E9`; toda la lógica nueva es aditiva y debe mantener esa suite en verde.

## 1. Hipótesis

- **H-AS4 (Confidencialidad):** el Gateway, con un IdP de prueba y un PDP externo, impide que un sujeto sin identidad válida, sin `scope` suficiente o sin relación `subject.customerId == owner.customerId` (salvo delegación explícita) acceda a perfiles o cotizaciones de otro cliente, y falla cerrado (deniega) si el PDP no responde.
- **H-AS8 (Integridad):** el evento `ProfileUpdated` viaja firmado (HMAC-SHA256) desde Perfilamiento hasta Cotización; cualquier alteración del payload, ausencia de firma, `keyId` desconocido o intento de publicar/consumir sin el rol ACL correcto en Redis Streams es detectado y rechazado antes de calcular la prima, quedando registrado en auditoría.

## 2. Componentes nuevos

| Componente | Rol | Ubicación propuesta |
|---|---|---|
| **IdP de prueba** | Emite JWT de prueba (HS256, `TEST ONLY`) con `sub`, `tenantId`, `scopes`, `exp`, `kid` | `security/idp/app.py`, servicio `idp` |
| **PDP / Autorizador** | Decide `PERMIT`/`DENY` dado `token` + `resource.owner.customerId` + `resource.tenantId` + acción; aplica ABAC por objeto y delegación | `security/pdp/app.py`, servicio `pdp` |
| **Gateway — middleware de seguridad** | Valida JWT (firma, `exp`, `aud`), resuelve el `owner` del recurso pedido, consulta al PDP, deniega por defecto si el PDP no responde | Nuevo módulo `gateway/security.py`, invocado al inicio de `proxy()` en `gateway/app.py` |
| **Perfilamiento — firmante** | Calcula `payloadHash` (HMAC-SHA256 sobre representación canónica) y agrega metadatos de integridad antes de publicar `ProfileUpdated` | Nueva función `sign_event()` en `profiling/app.py`, invocada dentro de `publish_profile_updated()` |
| **Cotización — verificador** | Verifica firma, `schemaVersion` y `keyId` antes de aplicar el evento; si falla, rechaza y NO llama a `repository.apply_event()` | Nueva función `verify_event()` en `quoting/app.py`, invocada dentro de `process_message()` |
| **Redis Streams — ACL por tópico** | Un usuario Redis `profile_producer` con permiso `+xadd` solo sobre `profile-updated`; un usuario `profile_consumer` con permiso `+xreadgroup`/`+xack` solo sobre ese stream; el resto de comandos denegados | `redis/users.acl`, montado en `redis-business` vía `--aclfile` |
| **Auditoría** | Log JSON estructurado (mismo patrón de `log_event`) de cada decisión PERMIT/DENY y cada evento aceptado/rechazado, sin PII más allá de `customerId` (ya usado hoy) | Reutiliza `log_event`; nuevo evento `AUTHZ_DECISION` y `INTEGRITY_CHECK` |
| **Runner de seguridad** | Ejecuta `SEC-C0`..`SEC-C9`, `SEC-I0`..`SEC-I9`, guarda `results/SEC-*.json` | Extensión de `scripts/run_experiment.py` (ver sección 6) |

Todas las llaves (HMAC, HS256 del IdP) se generan localmente, se marcan `TEST ONLY` en el propio valor (`"TEST-ONLY-..."`) y se referencian por `keyId`; nunca se usan credenciales reales.

## 3. Cambios de código puntuales

### 3.1 `gateway/app.py`

Insertar la validación **antes** de la llamada a `forward()` dentro de `proxy(subpath)`, sin tocar `choose_active()`, el failover ni el pipeline SHADOW:

```python
from security import authorize  # nuevo módulo gateway/security.py

@app.route("/<path:subpath>", methods=["GET", "POST", ...])
def proxy(subpath):
    decision = authorize(request, subpath)          # NUEVO
    if decision.status != "PERMIT":                 # NUEVO
        return jsonify(decision.body), decision.code # NUEVO
    # ... lógica existente de choose_active/forward/SHADOW sin cambios
```

`authorize()` extrae `Authorization: Bearer <jwt>`, valida firma/`exp`/`aud` contra el IdP de prueba (clave compartida `TEST ONLY`), determina el `owner.customerId` a partir del `subpath` (`/quotes/<id>`, `/profiles/<id>`), y llama al PDP con `{subject, resource, action}`. Si el PDP no responde en `PDP_TIMEOUT_MS`, la decisión es `DENY` (fail-closed), nunca `PERMIT` por omisión. Rutas `/admin/*`, `/gateway/status`, `/metrics`, `/health` quedan explícitamente fuera de esta capa (igual que hoy, son de experimento).

### 3.2 `profiling/app.py`

`publish_profile_updated()` (línea 299) agrega metadatos de integridad al evento antes de `publish()`:

```python
def canonical_payload(event: dict) -> bytes:
    core = {k: event[k] for k in ("eventId", "customerId", "version", "riskScore", "riskLevel", "timestamp")}
    return json.dumps(core, sort_keys=True, separators=(",", ":")).encode()

def sign_event(event: dict) -> dict:
    payload_hash = hmac.new(HMAC_KEY.encode(), canonical_payload(event), hashlib.sha256).hexdigest()
    event.update({
        "payloadHash": payload_hash,
        "producerId": "profiling",
        "keyId": HMAC_KEY_ID,          # p.ej. "test-key-2026-09"
        "algorithm": "HMAC-SHA256",
        "schemaVersion": "1.1",
    })
    return event
```

`HMAC_KEY`/`HMAC_KEY_ID` salen de variables de entorno (`TEST ONLY`, ya presentes en `docker-compose.yaml` como secreto de prueba, nunca hardcodeadas en el repo salvo un valor de ejemplo marcado).

### 3.3 `quoting/app.py`

`parse_event()` (línea 119) crece con los campos nuevos; `process_message()` (línea 132) verifica **antes** de `repository.apply_event()`:

```python
def verify_event(event: dict) -> tuple[bool, str]:
    if not all(k in event for k in ("payloadHash", "producerId", "keyId", "algorithm")):
        return False, "MISSING_INTEGRITY_FIELDS"
    if event["keyId"] not in KNOWN_KEYS:
        return False, "UNKNOWN_KEY"
    expected = hmac.new(KNOWN_KEYS[event["keyId"]].encode(), canonical_payload(event), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, event["payloadHash"]):
        return False, "SIGNATURE_MISMATCH"
    return True, "OK"

def process_message(message_id, fields, recovered=False):
    event = parse_event(fields)
    ok, reason = verify_event(event)
    if not ok:
        metrics.increment("integrity_rejected")
        log_event("INTEGRITY_CHECK", eventId=event["eventId"], producerId=event.get("producerId"),
                   keyId=event.get("keyId"), result="REJECTED", reason=reason)
        business.xack(STREAM_PROFILE_UPDATED, MATERIALIZER_GROUP, message_id)  # se descarta, no se reintenta
        return
    # ... resto igual (repository.apply_event, xack, log_event PROFILE_MATERIALIZED)
```

### 3.4 `docker-compose.yaml`

Agregar servicios `idp` (puerto 6100) y `pdp` (puerto 6200), variables `HMAC_KEY`/`HMAC_KEY_ID`/`KNOWN_KEYS_JSON` en `profiling` y `quoting-a`/`quoting-b`, variables `IDP_URL`/`PDP_URL`/`PDP_TIMEOUT_MS` en `gateway`, y un archivo `redis/users.acl` montado en `redis-business` con `--aclfile /usr/local/etc/redis/users.acl` que define `profile_producer` y `profile_consumer`. `redis-control` no cambia (sigue siendo pub/sub efímero sin ACL, fuera del alcance de AS-8).

## 4. Contrato de eventos — delta sobre `docs/events.md`

`ProfileUpdated` gana 5 campos (retrocompatibles, no se tocan `eventId`/`correlationId`/`version`):

```json
{
  "eventId": "uuid",
  "eventType": "ProfileUpdated",
  "correlationId": "uuid",
  "customerId": "C001",
  "version": "10",
  "riskScore": "40.0",
  "riskLevel": "MEDIUM",
  "timestamp": "2026-09-06T15:00:00+00:00",
  "payloadHash": "hmac-sha256 hex sobre representación canónica",
  "producerId": "profiling",
  "keyId": "test-key-2026-09",
  "algorithm": "HMAC-SHA256",
  "schemaVersion": "1.1"
}
```

## 5. Escenarios SEC-C0 a SEC-C9 (AS-4, Confidencialidad)

| ID | Given / When / Then | Evidencia esperada |
|---|---|---|
| SEC-C0 | JWT válido, `sub.customerId == owner.customerId`, scope correcto → `GET /quotes/C001` | `200`, `availability=100%` (baseline, análogo a E0) |
| SEC-C1 | Sin header `Authorization` → `GET /quotes/C001` | `401`, `reason=MISSING_TOKEN` |
| SEC-C2 | JWT con `exp` vencido → `GET /quotes/C001` | `401`, `reason=TOKEN_EXPIRED` |
| SEC-C3 | JWT válido de `C002` pidiendo `/quotes/C001` sin delegación | `403`, `reason=OWNERSHIP_MISMATCH` |
| SEC-C4 | JWT válido con `scopes=["profiles:read"]` (sin `quotes:read`) pidiendo `/quotes/C001` | `403`, `reason=INSUFFICIENT_SCOPE` |
| SEC-C5 | JWT de `C002` con `scopes=["delegated:C001"]` pidiendo `/quotes/C001` | `200`, decisión `PERMIT` con `reason=DELEGATION` en auditoría |
| SEC-C6 | JWT válido pero `tenantId` del token ≠ `tenantId` del recurso | `403`, `reason=TENANT_MISMATCH` |
| SEC-C7 | PDP detenido (`docker compose stop pdp`) y request con JWT válido | `403`/`503`, `reason=PDP_UNAVAILABLE` (fail-closed, nunca `200`) |
| SEC-C8 | JWT con firma alterada (un carácter cambiado) | `401`, `reason=INVALID_SIGNATURE` |
| SEC-C9 | Carga (`run_load`) con JWT válido durante 6s, igual que `E0` pero con la capa de autorización activa | `availability=100%`, overhead de latencia p95 reportado (regresión de rendimiento, no solo funcional) |

## 6. Escenarios SEC-I0 a SEC-I9 (AS-8, Integridad)

| ID | Given / When / Then | Evidencia esperada |
|---|---|---|
| SEC-I0 | Refresh normal de perfil → evento firmado, Cotización aplica | `decision=APPLIED`, `payloadHash` verificado (baseline) |
| SEC-I1 | Evento con `riskScore` alterado después de firmarlo (simulado desde el runner, releyendo el stream y republicando con XADD directo) manteniendo `payloadHash` original | `integrity_rejected>=1`, `reason=SIGNATURE_MISMATCH`, prima NO recalculada |
| SEC-I2 | Evento publicado sin `payloadHash`/`keyId` (bypass del firmante) | `reason=MISSING_INTEGRITY_FIELDS`, rechazado |
| SEC-I3 | Evento firmado con un `keyId` no presente en `KNOWN_KEYS` de Cotización | `reason=UNKNOWN_KEY`, rechazado |
| SEC-I4 | Cliente Redis sin credenciales `profile_producer` intenta `XADD profile-updated` | Redis responde `NOPERM`, evento nunca llega al stream |
| SEC-I5 | Cliente Redis sin credenciales `profile_consumer` intenta `XREADGROUP` sobre `profile-updated` | Redis responde `NOPERM` |
| SEC-I6 | Rotación: se agrega `keyId=test-key-2026-10` a `KNOWN_KEYS`, Perfilamiento firma con la nueva llave | `decision=APPLIED` con el nuevo `keyId`; eventos previos con la llave anterior siguen verificando (retrocompatibilidad) |
| SEC-I7 | Mismo `eventId` reenviado dos veces, ambos con firma válida | Primera vez `APPLIED`, segunda vez `decision=DUPLICATE` (idéntico a la lógica actual de `docs/events.md`, sin interferencia de la verificación de firma) |
| SEC-I8 | Carga sostenida (`run_load`) con verificación de firma activa en cada mensaje | `availability=100%`, overhead de throughput/latencia reportado frente a baseline `E0` |
| SEC-I9 | Se fuerzan 3 eventos con distintas causas de rechazo (I1, I2, I3) | `results/SEC-I9.json` referencia los 3 registros de auditoría (`INTEGRITY_CHECK`) con `producerId`/`keyId`/`reason`, sin exponer el HMAC key material |

## 7. Extensión del runner (`scripts/run_experiment.py`)

- Ampliar `HYPOTHESES`/`CRITERIA` con las 20 claves `SEC-C0`..`SEC-C9`, `SEC-I0`..`SEC-I9`.
- Agregar en `execute()` un bloque `if scenario.startswith("SEC-C")` / `startswith("SEC-I")` que despache a nuevas funciones en `scripts/security_scenarios.py` (nuevo módulo, para no inflar el archivo actual): `issue_token(sub, tenant, scopes, exp_delta)`, `call_with_token(method, url, token)`, `tamper_and_republish(event)`, `set_known_key(key_id, secret)`.
- Generalizar `write_acceptance_matrix()`: hoy itera `f"E{i}" for i in range(10)`; debe iterar sobre todos los `results/*.json` existentes (o una lista combinada `EXPERIMENT_IDS = [*E_IDS, *SEC_C_IDS, *SEC_I_IDS]`) para que la matriz de aceptación consolidada incluya ambos experimentos sin romper el CSV existente.
- `main()` acepta también `SEC-C<n>|SEC-I<n>|all-security|all` (donde `all` sigue corriendo solo `E0`-`E9` por compatibilidad, y se agrega `all-security` y `all-experiments`).
- Salida: `results/SEC-C<n>.json`, `results/SEC-I<n>.json`, filas nuevas en `results/summary.csv` y `results/acceptance_matrix.csv` (mismo formato, mismas columnas).

## 8. Pruebas automatizadas locales

Nuevo directorio `tests/security/` (mismo patrón que `tests/integration/test_running_stack.py`):

- `tests/security/test_confidentiality.py`: un test por `SEC-C0`..`SEC-C9`, marcado `@pytest.mark.integration`, reutilizando el fixture de stack levantado (`RUN_INTEGRATION=1 pytest -m integration tests/security`).
- `tests/security/test_integrity.py`: un test por `SEC-I0`..`SEC-I9`.
- `tests/unit/test_gateway_security.py` y `tests/unit/test_event_signing.py`: pruebas unitarias de `authorize()`, `sign_event()`, `verify_event()` sin levantar Docker (mock del PDP/IdP), para feedback rápido en CI.

## 9. No regresión de `E0`-`E9`

Antes de cerrar la implementación: `python scripts/run_experiment.py all` debe seguir devolviendo `accepted=true` en las 10 filas de `results/summary.csv`. La capa de seguridad no debe interceptar `/gateway/status`, `/metrics`, `/health` ni los endpoints `/admin/*` que usa el runner de disponibilidad; el checklist de PR incluye correr `all` seguido de `all-security` y adjuntar ambos `acceptance_matrix.csv`.

## 10. Fases de trabajo

1. **IdP + PDP + middleware del Gateway** (AS-4 de punta a punta) → validar con `SEC-C0`-`SEC-C8` manualmente vía `curl`.
2. **Firmante en Perfilamiento + verificador en Cotización + ACL de Redis** (AS-8) → validar con `SEC-I0`-`SEC-I7`.
3. **Extensión del runner** (`security_scenarios.py`, `HYPOTHESES`/`CRITERIA`, `write_acceptance_matrix` generalizado) → automatizar `SEC-C9`, `SEC-I8`, `SEC-I9` (carga y auditoría).
4. **Regresión** → correr `E0`-`E9` completos, corregir cualquier interferencia.
5. **Documentación y video** → actualizar `docs/events.md`, crear `docs/video-demo-security.md` (sección 11).

## 11. Guion de video (mirror de `docs/video-demo.md`)

Se crea `docs/video-demo-security.md` con la misma estructura: preparación (`docker compose up --build -d --wait`, `python scripts/run_experiment.py all-security`), introducción de la hipótesis AS-4/AS-8, demostración en vivo de `SEC-C1/C3/C7` (401/403/fail-closed) con `curl` mostrando el header `Authorization` y la respuesta, demostración de `SEC-I1/I4` (tampering y ACL) mostrando los logs `INTEGRITY_CHECK` y el rechazo de Redis `NOPERM`, cierre con `python scripts/print_summary.py` y `cat results/acceptance_matrix.csv` mostrando ambos experimentos (disponibilidad + seguridad) en verde.

## 12. Restricciones transversales

Todas las llaves HMAC y secretos del IdP están marcadas `TEST ONLY` y viven en variables de entorno de `docker-compose.yaml`, nunca en claro en el código fuente salvo un valor de ejemplo evidentemente ficticio. Los logs de auditoría solo incluyen `customerId`, `producerId`, `keyId`, `reason`, nunca el secreto HMAC ni el JWT completo. Toda ruta nueva bajo `/admin/*` (p. ej. para forzar rotación de llaves en las pruebas) queda explícitamente documentada como experimento, igual que las existentes. El PDP fail-closed es un invariante de diseño, no una configuración opcional: la ausencia de respuesta nunca deriva en `PERMIT`.
