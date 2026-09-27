# Guion para el video del experimento de seguridad (AS-4 / AS-8)

Duración sugerida: 8 a 10 minutos. Graba la pantalla a 1080p con tres terminales visibles: comandos, logs del Gateway y logs de Quoting.

## Preparación antes de grabar

```bash
docker compose down -v
docker compose up --build -d --wait
python scripts/run_experiment.py all
python scripts/run_experiment.py all-security
```

La primera ejecución construye también `idp` y `pdp`, así que hazla antes de empezar a grabar. `all` confirma que `E0`-`E9` siguen en verde con la capa de seguridad activa. `all-security` deja `results/SEC-*.json` como respaldo.

Variables de apoyo para los `curl`:

```bash
token() { curl -s -X POST http://localhost:6100/tokens -H 'Content-Type: application/json' -d "$1" \
  | python -c 'import sys,json;print(json.load(sys.stdin)["access_token"])'; }
C001=$(token '{"sub":"C001","scopes":["quotes:read"]}')
C002=$(token '{"sub":"C002","scopes":["quotes:read"]}')
```

Terminales de logs:

```bash
docker compose logs -f --no-log-prefix gateway | grep AUTHZ_DECISION
docker compose logs -f --no-log-prefix quoting-a quoting-b | grep INTEGRITY_CHECK
```

## Secuencia de grabación

### 1. Introducción, 1 minuto

Presenta las dos hipótesis:

- **H-AS4:** sin identidad válida, sin scope o sin ser dueño (salvo una delegación explícita) no se accede a la cotización de otro cliente, y si el PDP no responde el sistema falla cerrado.
- **H-AS8:** un `ProfileUpdated` alterado, sin firma o firmado con una llave desconocida se rechaza antes de calcular la prima, y la ACL de Redis impide publicar o consumir sin el rol correcto.

Muestra `docker compose ps` con `idp` y `pdp`, y la tabla de escenarios del README.

### 2. SEC-C1 y SEC-C3: 401 y 403, 2 minutos

```bash
curl -si http://localhost:8080/quotes/C001 | head -1; curl -s http://localhost:8080/quotes/C001; echo
curl -s http://localhost:8080/quotes/C001 -H "Authorization: Bearer $C001" | python -m json.tool
curl -si http://localhost:8080/quotes/C001 -H "Authorization: Bearer $C002" | head -1
curl -s  http://localhost:8080/quotes/C001 -H "Authorization: Bearer $C002"; echo
```

Señala que se ve el header `Authorization` en la petición, que responden `401 MISSING_TOKEN`, `200` para el dueño y `403 OWNERSHIP_MISMATCH` para C002, y que cada decisión aparece en la terminal de logs como `AUTHZ_DECISION` sin el token.

### 3. SEC-C7: fail-closed, 1 minuto 30 segundos

```bash
docker compose stop pdp
curl -si http://localhost:8080/quotes/C001 -H "Authorization: Bearer $C001" | head -1
curl -s  http://localhost:8080/quotes/C001 -H "Authorization: Bearer $C001"; echo
docker compose start pdp
```

Recalca que el token es válido y aun así la respuesta es `503 PDP_UNAVAILABLE`, nunca `200`. No existe una configuración que cambie esto.

### 4. SEC-I1: tampering del evento, 2 minutos

```bash
python scripts/run_experiment.py SEC-I1
python -m json.tool results/SEC-I1.json
```

Muestra en la terminal de Quoting las dos líneas `INTEGRITY_CHECK` con `result=REJECTED` y `reason=SIGNATURE_MISMATCH`, una por réplica. En el JSON, destaca que `views_before == views_after` y que la prima no cambió. Explica que el evento falsificado usaba un `eventId` nuevo y una `version` mayor: sin la verificación se habría aplicado.

### 5. SEC-I4: ACL de Redis, 1 minuto

```bash
docker compose exec -T redis-business redis-cli XADD profile-updated '*' eventId forged
docker compose exec -T redis-business redis-cli --no-auth-warning \
  --user profile_consumer --pass TEST-ONLY-redis-profile-consumer-pw-2026-09 \
  XADD profile-updated '*' eventId forged
```

Ambos responden `NOPERM`: ni un cliente anónimo ni el rol consumidor pueden publicar en `profile-updated`.

### 6. Evidencia y cierre, 1 minuto

```bash
python scripts/print_summary.py
cat results/acceptance_matrix.csv
```

Muestra que la matriz consolidada tiene las filas `E0`-`E9` y `SEC-C0`-`SEC-I9` en `PASS`. Cierra con la formulación académica: “El experimento aporta evidencia de que las tácticas de autorización y firma de eventos satisfacen AS-4 y AS-8 bajo las condiciones evaluadas”. Aclara que el IdP y las llaves son de prueba (`TEST ONLY`).

## Plan alterno si una demostración falla durante la grabación

Muestra los `results/SEC-*.json` de la ejecución de preparación y los logs de auditoría:

```bash
docker compose logs --since=10m --no-log-prefix gateway quoting-a quoting-b | grep -E 'AUTHZ_DECISION|INTEGRITY_CHECK'
```

Si SEC-C7 deja el PDP detenido, ejecuta `docker compose start pdp` antes de continuar. No edites resultados manualmente.
