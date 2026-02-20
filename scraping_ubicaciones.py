"""
scraping_ubicaciones.py
=======================
Bot Playwright para poblar la tabla `ubicaciones` en Supabase.

MODOS DE OPERACIÓN:
  1. Importación local: si existen archivos en ./excels/ cuyo nombre coincide
     con el texto_exacto de un medicamento en la cola, los procesa directamente.
  2. Scraping web: abre opm-digemid.minsa.gob.pe, busca el texto_exacto,
     filtra por AREQUIPA, descarga el Excel y lo procesa.

Rate-limit: Si el sitio devuelve 429 o detecta bloqueo → duerme 2 horas y rereinicia.
"""

import asyncio
import json
import os
import re
import random
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from supabase import create_client, Client

# ────────────────────────────────────────────
#  CONFIGURACIÓN
# ────────────────────────────────────────────
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

DIGEMID_URL  = "https://opm-digemid.minsa.gob.pe/#/consulta-producto"
CARPETA_EXCELS = Path(__file__).parent / "excels"
CARPETA_EXCELS.mkdir(exist_ok=True)

AREQUIPA_VALUE         = "04"       # Código del <select> departamento
COOLDOWN_SEGUNDOS      = 7200       # 2 horas si hay rate-limit
PAUSA_ENTRE_BUSQUEDAS  = (2, 5)     # Segundos aleatorios entre búsquedas

# ────────────────────────────────────────────
#  SUPABASE CLIENT
# ────────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ────────────────────────────────────────────
#  HELPERS
# ────────────────────────────────────────────
def sanitizar_nombre_archivo(texto: str) -> str:
    """Convierte texto_exacto en nombre de archivo válido para Windows."""
    return re.sub(r'[\\/:*?"<>|]', '_', texto)


def generar_url_maps(establecimiento: str, direccion: str, distrito: str, provincia: str) -> str:
    """
    Genera URL de búsqueda en Google Maps.
    Ejemplo: https://www.google.com/maps/search/?api=1&query=BOTICA+SOFIA,+TRUJILLO,+LA+LIBERTAD
    """
    partes = [p for p in [establecimiento, direccion, distrito, provincia]
              if p and str(p).strip()]
    query_texto = ", ".join(str(p).strip() for p in partes)
    return f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote_plus(query_texto)}"


def parsear_excel(ruta_excel: Path, medicamento_id: str) -> list[dict]:
    """
    Lee el Excel de DIGEMID (cabeceras en fila 8, datos desde fila 9).
    Columnas A–L:
      A: Tipo | B: Fecha Actualizac. | C: Nombre producto | D: Titular |
      E: Fabricante | F: Farmacia/Botica | G: Teléfono | H: Precio |
      I: Departamento | J: Provincia | K: Distrito | L: Dirección
    """
    try:
        df = pd.read_excel(ruta_excel, header=7, engine='openpyxl')  # 0-indexed: fila 8
    except Exception as e:
        print(f"   [X] Error al leer '{ruta_excel.name}': {e}")
        return []

    COLUMNAS = [
        "tipo", "fecha_actualizacion_digemid", "nombre_producto_scraping",
        "titular", "fabricante", "establecimiento", "telefono",
        "precio", "departamento", "provincia", "distrito", "direccion"
    ]
    df = df.iloc[:, :12]
    df.columns = COLUMNAS
    df = df.dropna(how='all')
    df = df[df['establecimiento'].notna() & (df['establecimiento'].astype(str).str.strip() != '')]

    registros = []
    for _, row in df.iterrows():
        establecimiento = str(row.get('establecimiento') or '').strip()
        direccion       = str(row.get('direccion') or '').strip()
        distrito        = str(row.get('distrito') or '').strip()
        provincia       = str(row.get('provincia') or '').strip()

        precio_raw = row.get('precio')
        try:
            precio = float(str(precio_raw).replace(',', '.')) if pd.notna(precio_raw) else None
        except (ValueError, TypeError):
            precio = None

        fecha_raw = row.get('fecha_actualizacion_digemid')
        if isinstance(fecha_raw, datetime):
            fecha_str = fecha_raw.isoformat()
        elif pd.notna(fecha_raw):
            fecha_str = str(fecha_raw)
        else:
            fecha_str = None

        registros.append({
            "medicamento_id":              medicamento_id,
            "tipo":                        str(row.get('tipo') or '').strip() or None,
            "fecha_actualizacion_digemid": fecha_str,
            "nombre_producto_scraping":    str(row.get('nombre_producto_scraping') or '').strip() or None,
            "titular":                     str(row.get('titular') or '').strip() or None,
            "fabricante":                  str(row.get('fabricante') or '').strip() or None,
            "establecimiento":             establecimiento or None,
            "telefono":                    str(row.get('telefono') or '').strip() or None,
            "precio":                      precio,
            "departamento":                str(row.get('departamento') or '').strip() or None,
            "provincia":                   provincia or None,
            "distrito":                    distrito or None,
            "direccion":                   direccion or None,
            "url_maps":                    generar_url_maps(establecimiento, direccion, distrito, provincia),
        })

    print(f"   [📊] {len(registros)} filas parseadas del Excel.")
    return registros


def tiene_ubicaciones(medicamento_id: str) -> bool:
    """Verifica si ya hay ubicaciones para este medicamento en la BD."""
    res = supabase.table("ubicaciones").select("id", count="exact").eq("medicamento_id", medicamento_id).execute()
    return (res.count or 0) > 0


def guardar_ubicaciones(registros: list[dict]) -> bool:
    """Inserta registros en la tabla ubicaciones por lotes de 500."""
    if not registros:
        print("   [!] Sin registros para guardar.")
        return False
    try:
        BATCH = 500
        for i in range(0, len(registros), BATCH):
            supabase.table("ubicaciones").insert(registros[i:i+BATCH]).execute()
        print(f"   [✅] {len(registros)} ubicaciones guardadas.")
        return True
    except Exception as e:
        print(f"   [X] Error al guardar: {e}")
        return False


def limpiar_cola(tarea_id: str):
    try:
        supabase.table("coln_procesamiento").delete().eq("id", tarea_id).execute()
        print(f"   [🗑️] Tarea {tarea_id[:8]}... eliminada de la cola.")
    except Exception as e:
        print(f"   [X] Error al limpiar cola: {e}")


def leer_cola_pendiente() -> list[dict]:
    try:
        res = supabase.table("coln_procesamiento")\
            .select("id, medicamento_id, payload")\
            .eq("tipo_tarea", "SCRAPING_UBICACION")\
            .eq("estado", "PENDIENTE")\
            .execute()
        return res.data or []
    except Exception as e:
        print(f"[X] Error al leer cola: {e}")
        return []


# ────────────────────────────────────────────
#  MODO 1: IMPORTACIÓN DE EXCELS LOCALES
# ────────────────────────────────────────────
def procesar_excels_locales(cola: list[dict]) -> list[str]:
    """
    Revisa ./excels/ y procesa archivos cuyo nombre (sin .xlsx) coincida
    con el texto_exacto de alguna tarea.
    """
    procesadas = []

    # Mapa: nombre_saneado → tarea
    mapa = {}
    for t in cola:
        payload = t.get("payload") or {}
        texto   = payload.get("texto_exacto", "")
        if texto:
            mapa[sanitizar_nombre_archivo(texto)] = t

    for archivo in CARPETA_EXCELS.glob("*.xlsx"):
        if archivo.stem in mapa:
            tarea = mapa[archivo.stem]
            mid   = tarea["medicamento_id"]
            tid   = tarea["id"]
            print(f"\n[📂] Excel local: '{archivo.name}'")

            if tiene_ubicaciones(mid):
                print("   [⏩] Ya tiene ubicaciones → limpiando.")
                archivo.unlink()
                limpiar_cola(tid)
                procesadas.append(tid)
                continue

            registros = parsear_excel(archivo, mid)
            if registros and guardar_ubicaciones(registros):
                archivo.unlink()
                limpiar_cola(tid)
                procesadas.append(tid)
            else:
                print("   [!] No procesado. Archivo conservado.")

    return procesadas


# ────────────────────────────────────────────
#  MODO 2: SCRAPING WEB
# ────────────────────────────────────────────
async def scraping_digemid(cola: list[dict]):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()

        await page.goto(DIGEMID_URL)
        await page.wait_for_load_state("networkidle")

        # Cerrar modales iniciales
        for btn in ["Cerrar", "Aceptar", "OK"]:
            try:
                await page.click(f"button:has-text('{btn}')", timeout=2000)
            except:
                pass

        for tarea in cola:
            tid  = tarea["id"]
            mid  = tarea["medicamento_id"]
            payload      = tarea.get("payload") or {}
            texto_exacto = payload.get("texto_exacto", "")

            if not texto_exacto:
                print(f"\n[!] Tarea {tid[:8]} sin texto_exacto. Skip.")
                continue

            print(f"\n[🔍] '{texto_exacto}'")

            if tiene_ubicaciones(mid):
                print("   [⏩] Ya tiene ubicaciones → limpiando cola.")
                limpiar_cola(tid)
                continue

            exito    = False
            intentos = 0

            while intentos < 3 and not exito:
                try:
                    await page.goto(DIGEMID_URL)
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(1.5)

                    for btn in ["Cerrar", "Aceptar"]:
                        try:
                            await page.click(f"button:has-text('{btn}')", timeout=1500)
                        except:
                            pass

                    # ── Escribir en buscador ──────────────────────────────
                    sel_input = "input[type='text']"
                    await page.wait_for_selector(sel_input, timeout=8000)
                    await page.fill(sel_input, "")
                    await asyncio.sleep(0.4)

                    await page.fill(sel_input, texto_exacto[:-1])

                    # Escuchar respuesta de autocomplete buscando 429
                    rate_limited = False
                    try:
                        async with page.expect_response(
                            lambda r: "autocompleteciudadano" in r.url, timeout=8000
                        ) as resp_info:
                            await page.type(sel_input, texto_exacto[-1], delay=random.randint(100, 300))

                        resp = await resp_info.value
                        if resp.status == 429:
                            rate_limited = True

                    except Exception as autocomplete_ex:
                        # Si la conexión se cerró (429 de Cloudflare cierra el socket)
                        # re-lanzar para que el except externo haga el cooldown + restart
                        err_str = str(autocomplete_ex)
                        if any(k in err_str for k in [
                            "Connection closed", "TargetClosedError",
                            "Target closed", "Browser closed", "context or brow"
                        ]):
                            raise  # -> lo atrapa el except externo con cooldown 2h
                        # Timeout o sin red de autocomplete → continuar normalmente

                    if rate_limited:
                        print(f"\n   [🚨] Rate limit (429). Esperando {COOLDOWN_SEGUNDOS//3600}h...")
                        await asyncio.sleep(COOLDOWN_SEGUNDOS)
                        intentos += 1
                        continue

                    # Detectar mensaje de bloqueo en UI
                    for sel_err in ["text=demasiadas solicitudes", "text=too many requests",
                                    "text=Servicio no disponible"]:
                        try:
                            if await page.is_visible(sel_err, timeout=800):
                                print(f"\n   [🚨] Bloqueo UI. Esperando {COOLDOWN_SEGUNDOS//3600}h...")
                                await asyncio.sleep(COOLDOWN_SEGUNDOS)
                                rate_limited = True
                                break
                        except:
                            pass

                    if rate_limited:
                        intentos += 1
                        continue

                    # ── Esperar y hacer click en la 1ra opción del dropdown ──
                    # Confirmado con inspección en vivo: DIGEMID usa Angular con esta estructura:
                    #   <ul class="dropdown-menu show" scrollable="true">
                    #     <li class="ng-star-inserted">
                    #       <div class="ng-star-inserted">
                    #         <a class="ng-tns-c0-0 ng-star-inserted">TEXTO</a>
                    #       </div>
                    #     </li>
                    #   </ul>
                    dropdown_visible = False
                    try:
                        # Esperar hasta 4s a que aparezca el contenedor del dropdown
                        await page.wait_for_selector("ul.dropdown-menu.show", timeout=4000, state="visible")
                        # Obtener todos los items clicables
                        items = await page.query_selector_all("ul.dropdown-menu.show a.ng-star-inserted")
                        if items:
                            await items[0].click()
                            dropdown_visible = True
                            print(f"   [✓] Opción seleccionada del autocomplete.")
                    except Exception as wait_ex:
                        err_str = str(wait_ex)
                        if any(k in err_str for k in [
                            "Connection closed", "TargetClosedError",
                            "Target closed", "Browser closed", "context or brow"
                        ]):
                            raise
                        pass

                    if not dropdown_visible:
                        # No apareció dropdown en 4s: error transitorio (429 previo, red lenta, etc.)
                        # → reintento en lugar de borrar de la cola
                        print(f"   [⚠️] Sin opciones en autocomplete para '{texto_exacto}'. Reintentando...")
                        intentos += 1
                        continue

                    await asyncio.sleep(0.5)

                    # ── Seleccionar Arequipa ──────────────────────────────
                    try:
                        await page.select_option(
                            "select[name='codigoDepartamento']",
                            value=AREQUIPA_VALUE,
                            timeout=5000
                        )
                        print("   [🗺️] Arequipa seleccionada.")
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        print(f"   [!] Filtro Arequipa no disponible: {e}")

                    # ── Click en Buscar ───────────────────────────────────
                    for sel_btn in ["button:has-text('Buscar')", "button[type='submit']"]:
                        try:
                            await page.click(sel_btn, timeout=3000)
                            break
                        except:
                            pass

                    await asyncio.sleep(3.5)

                    # Verificar sin resultados
                    sin_res = False
                    for msg in ["No se encontraron resultados", "sin resultados", "0 registros"]:
                        try:
                            if await page.is_visible(f"text={msg}", timeout=1500):
                                sin_res = True
                                break
                        except:
                            pass

                    if sin_res:
                        print(f"   [📭] Sin resultados en Arequipa para '{texto_exacto}'.")
                        limpiar_cola(tid)
                        exito = True
                        continue

                    # ── Descargar Excel ───────────────────────────────────
                    nombre_archivo = sanitizar_nombre_archivo(texto_exacto) + ".xlsx"
                    ruta_destino   = CARPETA_EXCELS / nombre_archivo

                    descargado = False
                    for sel_xls in [
                        "button:has-text('Excel')",
                        "a:has-text('Excel')",
                        "button[title*='Excel']",
                        "button:has-text('Exportar')",
                        "button:has-text('Descargar')",
                    ]:
                        try:
                            async with page.expect_download(timeout=25000) as dl_info:
                                await page.click(sel_xls, timeout=3000)
                            dl = await dl_info.value
                            await dl.save_as(ruta_destino)
                            print(f"   [📥] Descargado → '{nombre_archivo}'")
                            descargado = True
                            break
                        except:
                            continue

                    if not descargado:
                        print(f"   [!] No se pudo descargar el Excel.")
                        intentos += 1
                        continue

                    # ── Guardar en BD ─────────────────────────────────────
                    registros = parsear_excel(ruta_destino, mid)
                    if registros and guardar_ubicaciones(registros):
                        ruta_destino.unlink()
                        limpiar_cola(tid)
                        exito = True
                    else:
                        print("   [!] Fallo al guardar. Excel conservado.")
                        intentos += 1

                except Exception as e:
                    error_msg = str(e)
                    # ── Detectar cierre de conexión / browser crash ───────
                    # Esto indica bloqueo del sitio: parar scraping, esperar 2h, reiniciar browser
                    if any(k in error_msg for k in [
                        "Connection closed", "TargetClosedError",
                        "Target closed", "Browser closed", "context or brow"
                    ]):
                        print(f"\n   [🚨] Conexión cerrada por el sitio (posible bloqueo o 429).")
                        print(f"   [🛑] Abortando scraping web de inmediato.")
                        try:
                            await browser.close()
                        except:
                            pass
                        return  # Termina completamente la funcion scraping_digemid
                    else:
                        print(f"   [X] Error inesperado: {e}")
                        intentos += 1
                        await asyncio.sleep(3)
                finally:
                    if not exito:
                        pausa = random.uniform(*PAUSA_ENTRE_BUSQUEDAS)
                        await asyncio.sleep(pausa)

            if not exito:
                print(f"   [!] Omitido tras 3 intentos.")
                supabase.table("coln_procesamiento")\
                    .update({"estado": "ERROR", "error_log": "Máximos intentos superados"})\
                    .eq("id", tid).execute()

            await asyncio.sleep(random.uniform(*PAUSA_ENTRE_BUSQUEDAS))

        await browser.close()
        print("\n[🎉] Scraping finalizado.")


# ────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────
async def main():
    print("=" * 60)
    print("  SCRAPING UBICACIONES — DIGEMID Perú")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] Faltan variables SUPABASE_URL / SUPABASE_KEY en .env")
        return

    print("\n[1] Leyendo cola de procesamiento...")
    cola = leer_cola_pendiente()
    print(f"    {len(cola)} tareas pendientes.")

    print("\n[2] Procesando Excels locales en ./excels/ ...")
    ya_procesadas = procesar_excels_locales(cola)
    if ya_procesadas:
        print(f"    {len(ya_procesadas)} procesadas desde archivos locales.")

    cola_restante = [t for t in cola if t["id"] not in ya_procesadas]

    if cola_restante:
        print(f"\n[3] Scraping web para {len(cola_restante)} medicamentos...")
        await scraping_digemid(cola_restante)
    else:
        print("\n[3] Sin pendientes para scraping web.")

    print(f"\n[✅] Todo finalizado. {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())
