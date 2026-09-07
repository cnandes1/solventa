# Informe de implementación

## Diagnóstico original

| Componente | Qué hacía | Qué estaba bien | Cambio necesario |
|---|---|---|---|
| `gateway/app.py` | Round-robin y Ping-Echo sobre un Redis | Estados ACTIVE/DOWN/SHADOW básicos | Separar control, reconectar, failover único y shadow traffic verificable |
| `profiling/app.py` | Perfilamiento: Circuit Breaker y A/B/C en hilos locales | Timeout HTTP y fallback iniciales | Votación mediante Streams, consenso tolerante, mocks determinísticos y métricas |
| `quoting/app.py` | Cotización: consumía `ProfileUpdated` y consultaba Redis | Versionado y ACK iniciales | SQLite por réplica, grupos independientes, transacción e integración sin HTTP |
| `risk_provider/app.py` | Simulaba latencia/falla por variables | Mock pequeño y aislado | Modos administrables, score estable y escenarios reproducibles |
| `docker-compose.yaml` | Redis único y dos réplicas | Stack local sencillo | Redis Business/Control, AOF y volúmenes SQLite independientes |
| `experiment/` | Scripts manuales secuenciales | Primeros escenarios documentados | Runner E0-E9, carga concurrente, JSON/CSV y criterios PASS/FAIL |

## Problemas corregidos

1. La vista compartida en Redis fue reemplazada por SQLite local en cada réplica.
2. El consumer group único de Cotización fue reemplazado por grupos `quoting-a-materializer` y `quoting-b-materializer`.
3. La aplicación de eventos ahora es transaccional, idempotente y resistente a eventos fuera de orden.
4. El journey rechaza perfiles inexistentes o vencidos; ya no inventa un score exitoso.
5. Cotización publica `ProfileRefreshRequested`; el journey basado en eventos no llama a Perfilamiento por HTTP.
6. Redis Business y Redis Control son servicios distintos; Business usa AOF y volumen.
7. Los pendientes se reclaman con `XAUTOCLAIM` y un idle configurable.
8. Open Finance ofrece modos determinísticos y el Circuit Breaker permite una sola prueba HALF_OPEN.
9. El Gateway hace un failover de transporte y excluye instancias DOWN.
10. SHADOW ejecuta comparaciones no autoritativas antes de la promoción.
11. A/B/C consumen eventos, publican resultados y el Validator decide por `correlationId`.
12. El experimento produce métricas, JSON por escenario y matrices CSV.

## Archivos principales creados

```text
docs/
  events.md
  implementation-report.md
  video-demo.md
gateway/
  state.py
load-tests/
  locustfile.py
  requirements.txt
profiling/
  circuit_breaker.py
  voting.py
quoting/
  storage.py
results/
  E0.json ... E9.json
  acceptance_matrix.csv
  summary.csv
scripts/
  print_summary.py
  run_experiment.py
tests/
  architecture/test_experiment_architecture.py
  integration/test_running_stack.py
  unit/test_circuit_breaker.py
  unit/test_health_state_machine.py
  unit/test_profile_versioning.py
  unit/test_voting.py
.env.example
pytest.ini
requirements-dev.txt
```

## Límites del resultado

El experimento valida comportamiento local bajo fallas controladas. No evalúa un clúster Redis, múltiples regiones, seguridad, capacidad de producción ni disponibilidad mensual. Los resultados deben interpretarse junto con la configuración, duración y carga de cada ejecución.
