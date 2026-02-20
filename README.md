# camera-doc-assistant-scraping

Bot de scraping para poblar la tabla `ubicaciones` en Supabase desde el portal DIGEMID.

## Configuración

1. Copia `.env.example` → `.env` y rellena las credenciales de Supabase:

```
SUPABASE_URL=https://...supabase.co
SUPABASE_KEY=eyJ...
```

2. Instala dependencias:

```bash
pip install -r requirements.txt
playwright install chromium
```

## Uso

### Scraping automático (desde la cola)

```bash
python scraping_ubicaciones.py
```

El bot leerá la tabla `coln_procesamiento` (tipo `SCRAPING_UBICACION`, estado `PENDIENTE`),
hará scraping en DIGEMID filtrando por **Arequipa**, descargará los Excels y guardará los resultados en `ubicaciones`.

### Importación manual de Excels

Si ya tienes Excels descargados, colócalos en la carpeta `./excels/` con el nombre:

```
{texto_exacto_busqueda}.xlsx
```

Por ejemplo: `ACETALITO 120 mg_ 5 mL Solucion - Suspension.xlsx`

El bot los detectará automáticamente al iniciar y los procesará sin hacer scraping web.

## Estructura

```
camera-doc-assistant-scraping/
├── scraping_ubicaciones.py   ← Script principal
├── excels/                   ← Carpeta para Excels (creada automáticamente)
├── requirements.txt
├── .env                      ← Credenciales (NO subir al repo)
└── .env.example
```

## Rate Limiting

El portal DIGEMID limita la cantidad de búsquedas. Cuando el bot detecta un error 429
o un mensaje de bloqueo, **espera automáticamente 2 horas** y luego continúa.
