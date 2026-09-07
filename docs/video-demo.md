# Guion para el video del experimento

Duración sugerida: 10 a 12 minutos. Graba la pantalla a 1080p y utiliza tres terminales visibles: carga, estado del Gateway y comandos de falla.

## Preparación antes de grabar

```bash
cd "/Users/davidcombita/Documents/ChatGPT/Proyecto Arquitectura/solventa-mejoras"
docker compose down -v
docker compose up --build -d --wait
python scripts/run_experiment.py E0
```

La primera ejecución descarga imágenes y construye contenedores. Hazla antes de iniciar la grabación.

## Secuencia de grabación

### 1. Introducción, 45 segundos

Explica la hipótesis: Cotización conserva disponibilidad porque consulta una vista SQLite local. Redis Business transfiere el estado y Redis Control detecta réplicas caídas.

Muestra brevemente `docker compose ps` y el diagrama Mermaid del README.

### 2. Estado inicial y transferencia de estado, 1 minuto

```bash
curl -s -X POST http://localhost:7500/profiles/C001/refresh | python -m json.tool
curl -s http://localhost:7002/materialized-profiles/C001 | python -m json.tool
curl -s http://localhost:7001/materialized-profiles/C001 | python -m json.tool
```

Señala que ambas réplicas tienen la misma versión, pero cada una usa un volumen y una base SQLite diferentes.

### 3. E3, Perfilamiento DOWN, 2 minutos

```bash
python scripts/run_experiment.py E3
cat results/E3.json
```

Explica que `/quotes/C001` continúa con `DEGRADED_SUCCESS`, mientras `/sync/quotes/C001` falla. Esa comparación muestra la diferencia entre la arquitectura EDA y la línea base síncrona.

### 4. E5 y E6, retiro y reintegro, 3 minutos

Terminal de estado:

```bash
watch -n 1 'curl -s http://localhost:8080/gateway/status | python -m json.tool'
```

Terminal de prueba:

```bash
python scripts/run_experiment.py E5
python scripts/run_experiment.py E6
```

Destaca la secuencia `ACTIVE`, `DOWN`, `SHADOW`, `ACTIVE`. Durante SHADOW la respuesta autoritativa sigue viniendo de A y B solo recibe validaciones.

### 5. E7 y E8, votación, 2 minutos

```bash
python scripts/run_experiment.py E7
python scripts/run_experiment.py E8
cat results/E7.json
cat results/E8.json
```

En E7 muestra que A=40 y B=40 forman consenso y C=90 queda como outlier. En E8 muestra `missingStrategies=["C"]`, `voteIncomplete=true` y una duración acotada por el timeout.

### 6. E9, caída de Redis Business, 1 minuto

```bash
python scripts/run_experiment.py E9
cat results/E9.json
```

Aclara que Redis Control sigue activo y que las cotizaciones existentes salen desde SQLite. El experimento no demuestra alta disponibilidad de un clúster Redis; demuestra desacoplamiento del read model local frente a una caída temporal del bus de negocio.

### 7. Evidencia y cierre, 1 minuto

```bash
python scripts/print_summary.py
docker compose ps
```

Concluye usando la formulación académica: “El experimento aporta evidencia de que la decisión arquitectónica satisface el comportamiento esperado bajo las condiciones evaluadas”. No extrapoles los porcentajes de una ejecución corta a disponibilidad mensual de producción.

## Plan alterno si una demostración falla durante la grabación

Conserva `results/*.json` de una ejecución exitosa anterior y muestra también los logs estructurados:

```bash
docker compose logs --since=10m gateway profiling quoting-a quoting-b
```

Reinicia el entorno con `docker compose down -v` y `docker compose up --build -d --wait` antes de repetir. No edites resultados manualmente.
