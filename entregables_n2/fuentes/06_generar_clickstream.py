#!/usr/bin/env python3
"""
===============================================================================
 Archivo      : 06_generar_clickstream.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Fuente (clickstream de la app móvil) · paso 06
 Propósito    : Generar una MUESTRA realista de eventos de la app móvil en
                formato .jsonl (un evento JSON por línea), coherente con los
                datos de Azure SQL, para probar la ingesta con Auto Loader.

 Coherencia con el OLTP (si se conecta a Azure SQL, opción por defecto)
   - customer_id y product_id existen de verdad en dbo.Customers / dbo.Products.
   - Cada pedido del canal 'app' del día genera una sesión que termina en un
     evento 'purchase' con su order_id, su cliente y sus productos reales
     → en Silver/Gold se puede cruzar el embudo de la app con las ventas.

 Casos borde sembrados a propósito (para Bronze/Silver)
   - duplicados       : el mismo event_id enviado 2 veces (reintento de la app)
   - tardíos          : eventos de días anteriores que llegan hoy (celular offline)
   - anónimos         : customer_id = null (navega sin iniciar sesión)
   - campo nuevo      : 'campaign' aparece en algunos eventos (versión nueva de la app)
   - tipo incorrecto  : price como texto ("64.90") → _rescued_data en Auto Loader
   - huérfanos        : product_id que no existe en el catálogo
   - JSON corrupto    : líneas truncadas → Auto Loader las marca como corruptas
   - reloj adelantado : event_ts unos minutos en el futuro respecto a received_at

 Uso
   python fuentes/06_generar_clickstream.py                       # hoy, lee IDs de Azure SQL
   python fuentes/06_generar_clickstream.py --fecha 2026-10-12
   python fuentes/06_generar_clickstream.py --sin-db              # sin conectarse (IDs por rango)

 Entradas     : Azure SQL (solo lectura, usuario app_andina + keyring, ver 02) o --sin-db
 Salidas      : fuentes/salida/clickstream/fecha=AAAA-MM-DD/eventos_AAAA-MM-DD_HH.jsonl (1 archivo por hora)
                fuentes/logs/clickstream_<fecha>_<hora>.json (resumen con los casos borde sembrados)
 Siguiente    : subir la carpeta fecha=… al volume /Volumes/andina_dev/landing/clickstream/
 Dependencias : Python 3.10+ ; pyodbc y keyring solo si se conecta a la base
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-12
===============================================================================
"""

from __future__ import annotations

import argparse
import importlib
import json
import random
import sys
import time
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# =============================================================================
# 1. CONFIGURACIÓN · proporciones de los casos borde (fáciles de justificar)
# =============================================================================
P_ANONIMO = 0.15         # sesiones sin login
P_DUPLICADO = 0.02       # eventos reenviados con el mismo event_id
P_TARDIO = 0.03          # eventos que ocurrieron 1–3 días antes de llegar
P_CAMPANIA = 0.05        # eventos con el campo nuevo 'campaign'
P_PRECIO_TEXTO = 0.01    # price enviado como string
P_HUERFANO = 0.005       # product_id inexistente
P_CORRUPTO = 0.003       # línea JSON truncada
P_RELOJ_ADELANTADO = 0.01

# Zona horaria del celular según país (octubre: Chile en horario de verano)
OFFSET_PAIS = {"PE": -5, "CO": -5, "EC": -5, "BO": -4, "CL": -3}
LIMA = timezone(timedelta(hours=-5))
# Peso de cada hora local en el tráfico (más uso en la noche)
PESO_HORA = [1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 7, 8, 7, 6, 6, 7, 8, 10, 12, 12, 10, 6, 3]
VERSIONES_APP = ["3.4.0", "3.4.1", "3.5.0"]          # 3.5.0 es la que envía 'campaign'
TERMINOS = ["zapatillas", "audifonos", "polo", "laptop", "mochila", "cafe", "licuadora", "casaca"]
CAMPANIAS = ["cyber_andina", "push_recompra", "email_bienvenida"]


# =============================================================================
# 2. DATOS DE REFERENCIA · desde Azure SQL o por rango (--sin-db)
# =============================================================================
def cargar_referencia(fecha: date, sin_db: bool) -> dict:
    """Devuelve clientes, productos y pedidos 'app' del día (en hora de Lima)."""
    if sin_db:
        return {"clientes": [(i, "PE") for i in range(1, 5076)],
                "productos": [(i, round(random.uniform(10, 900), 2)) for i in range(1, 401)],
                "pedidos_app": []}

    # Reutiliza la conexión del generador 02 (app_andina + clave en keyring).
    sys.path.insert(0, str(Path(__file__).parent))
    gen = importlib.import_module("02_generar_datos_andina")
    conn = gen.conectar()
    cur = conn.cursor()

    cur.execute("SELECT CustomerID, CountryCode FROM dbo.Customers WHERE IsActive = 1")
    clientes = [(r[0], r[1]) for r in cur.fetchall()]
    cur.execute("SELECT ProductID, CAST(Price AS FLOAT) FROM dbo.Products WHERE Status = 'activo'")
    productos = [(r[0], r[1]) for r in cur.fetchall()]

    # Día de Lima = [fecha 05:00 UTC, fecha+1 05:00 UTC); OrderDate se guarda en UTC.
    ini = datetime(fecha.year, fecha.month, fecha.day, 5, 0)
    cur.execute("""SELECT o.OrderID, o.CustomerID, c.CountryCode, o.OrderDate,
                          CAST(o.TotalAmount AS FLOAT), oi.ProductID, oi.Quantity, CAST(oi.UnitPrice AS FLOAT)
                   FROM dbo.Orders o
                   JOIN dbo.Customers c  ON c.CustomerID = o.CustomerID
                   JOIN dbo.OrderItems oi ON oi.OrderID = o.OrderID
                   WHERE o.Channel = 'app' AND o.OrderDate >= ? AND o.OrderDate < DATEADD(DAY, 1, ?)""", ini, ini)
    pedidos = defaultdict(lambda: {"items": []})
    for oid, cid, pais, fec, total, pid, qty, precio in cur.fetchall():
        p = pedidos[oid]
        p.update(order_id=oid, customer_id=cid, pais=pais,
                 order_ts=fec.replace(tzinfo=timezone.utc), total=total)
        p["items"].append((pid, qty, precio))
    conn.close()
    gen.log(f"Referencia: {len(clientes):,} clientes · {len(productos):,} productos · "
            f"{len(pedidos):,} pedidos 'app' del {fecha}")
    return {"clientes": clientes, "productos": productos, "pedidos_app": list(pedidos.values())}


# =============================================================================
# 3. GENERACIÓN DE SESIONES · embudo app_open → … → purchase
# =============================================================================
class Generador:
    def __init__(self, ref: dict, fecha: date, rng: random.Random):
        self.ref, self.fecha, self.rng = ref, fecha, rng
        self.eventos: list[dict] = []
        self.casos = Counter()

    # --- utilidades ---------------------------------------------------------
    def _hora_aleatoria_utc(self) -> datetime:
        """Instante del día de Lima elegido con más peso en la noche, devuelto en UTC."""
        h = self.rng.choices(range(24), weights=PESO_HORA)[0]
        local = datetime(self.fecha.year, self.fecha.month, self.fecha.day, h,
                         self.rng.randint(0, 59), self.rng.randint(0, 59), tzinfo=LIMA)
        return local.astimezone(timezone.utc)

    def _evento(self, sesion: dict, tipo: str, ts_utc: datetime, **extra) -> None:
        """Arma un evento con la hora local del celular (event_ts) y la de llegada al servidor (received_at)."""
        tz = timezone(timedelta(hours=OFFSET_PAIS.get(sesion["pais"], -5)))
        recibido = ts_utc + timedelta(seconds=self.rng.randint(1, 20))
        ev = {"event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
              "event_type": tipo,
              "event_ts": ts_utc.astimezone(tz).isoformat(timespec="milliseconds"),
              "received_at": recibido.isoformat(timespec="milliseconds"),
              "session_id": sesion["session_id"],
              "customer_id": sesion["customer_id"],
              "device_id": sesion["device_id"],
              "platform": sesion["platform"],
              "app_version": sesion["app_version"],
              "country": sesion["pais"],
              **extra}
        # caso borde: la versión 3.5.0 empieza a enviar un campo nuevo
        if sesion["app_version"] == "3.5.0" and self.rng.random() < P_CAMPANIA * 4:
            ev["campaign"] = self.rng.choice(CAMPANIAS); self.casos["campo_nuevo_campaign"] += 1
        self.eventos.append(ev)

    def _nueva_sesion(self, customer_id, pais) -> dict:
        return {"session_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
                "customer_id": customer_id, "pais": pais,
                "device_id": f"dev-{self.rng.getrandbits(40):010x}",
                "platform": self.rng.choice(["android"] * 7 + ["ios"] * 3),
                "app_version": self.rng.choice(VERSIONES_APP)}

    def _producto(self) -> tuple[int, float]:
        if self.rng.random() < P_HUERFANO:                    # caso borde: no existe en el catálogo
            self.casos["product_id_huerfano"] += 1
            return (999_000 + self.rng.randint(1, 999), 49.90)
        return self.rng.choice(self.ref["productos"])

    # --- sesiones -------------------------------------------------------------
    def sesion_con_compra(self, pedido: dict) -> None:
        """Sesión que termina en un pedido REAL del canal app (mismo cliente, productos y order_id)."""
        s = self._nueva_sesion(pedido["customer_id"], pedido["pais"])
        fin = pedido["order_ts"]
        t = fin - timedelta(minutes=self.rng.randint(4, 25))
        self._evento(s, "app_open", t)
        for pid, qty, precio in pedido["items"]:
            t += timedelta(seconds=self.rng.randint(20, 120)); self._evento(s, "product_view", t, product_id=pid, price=precio)
            t += timedelta(seconds=self.rng.randint(10, 60));  self._evento(s, "add_to_cart", t, product_id=pid, quantity=qty, price=precio)
        self._evento(s, "checkout_start", min(t + timedelta(seconds=30), fin - timedelta(seconds=5)))
        self._evento(s, "purchase", fin, order_id=pedido["order_id"], total_amount=pedido["total"],
                     items=len(pedido["items"]))
        self.casos["sesiones_con_compra_real"] += 1

    def sesion_navegacion(self) -> None:
        """Sesión que abandona en algún punto del embudo (la mayoría)."""
        anonimo = self.rng.random() < P_ANONIMO
        cid, pais = (None, "PE") if anonimo else self.rng.choice(self.ref["clientes"])
        if anonimo:
            self.casos["sesiones_anonimas"] += 1
        s = self._nueva_sesion(cid, pais)
        t = self._hora_aleatoria_utc()
        self._evento(s, "app_open", t)
        if self.rng.random() < 0.4:
            t += timedelta(seconds=self.rng.randint(5, 40))
            self._evento(s, "search", t, search_term=self.rng.choice(TERMINOS))
        carrito = []
        for _ in range(self.rng.randint(1, 6)):
            pid, precio = self._producto()
            t += timedelta(seconds=self.rng.randint(10, 150))
            self._evento(s, "product_view", t, product_id=pid, price=precio)
            if self.rng.random() < 0.25:
                t += timedelta(seconds=self.rng.randint(5, 60))
                self._evento(s, "add_to_cart", t, product_id=pid, quantity=self.rng.randint(1, 3), price=precio)
                carrito.append(pid)
        if carrito and self.rng.random() < 0.3:
            t += timedelta(seconds=self.rng.randint(5, 60))
            self._evento(s, "remove_from_cart", t, product_id=carrito[-1])
        if carrito and self.rng.random() < 0.25:               # llega al checkout pero no paga
            t += timedelta(seconds=self.rng.randint(20, 90))
            self._evento(s, "checkout_start", t)

    # --- casos borde a nivel de evento ---------------------------------------
    def aplicar_casos_borde(self) -> None:
        for ev in self.eventos:
            if "price" in ev and self.rng.random() < P_PRECIO_TEXTO:     # tipo incorrecto
                ev["price"] = f"{ev['price']:.2f}"; self.casos["price_como_texto"] += 1
            if ev["event_type"] == "purchase":                           # la compra debe cuadrar con el pedido real
                continue
            if self.rng.random() < P_TARDIO:                             # ocurrió días antes
                dias = self.rng.randint(1, 3)
                ts = datetime.fromisoformat(ev["event_ts"]) - timedelta(days=dias)
                ev["event_ts"] = ts.isoformat(timespec="milliseconds"); self.casos["eventos_tardios"] += 1
            elif self.rng.random() < P_RELOJ_ADELANTADO:                 # reloj del celular adelantado
                ts = datetime.fromisoformat(ev["event_ts"]) + timedelta(minutes=self.rng.randint(3, 15))
                ev["event_ts"] = ts.isoformat(timespec="milliseconds"); self.casos["reloj_adelantado"] += 1
        duplicados = [dict(e) for e in self.eventos if self.rng.random() < P_DUPLICADO]
        for d in duplicados:                                             # mismo event_id, llega después
            d["received_at"] = (datetime.fromisoformat(d["received_at"])
                                + timedelta(seconds=self.rng.randint(30, 600))).isoformat(timespec="milliseconds")
        self.eventos.extend(duplicados); self.casos["eventos_duplicados"] = len(duplicados)


# =============================================================================
# 4. ESCRITURA · un .jsonl por hora de llegada (como lo depositaría un colector)
# =============================================================================
def escribir(eventos: list[dict], fecha: date, salida: Path, rng: random.Random) -> tuple[list[Path], int]:
    carpeta = salida / "clickstream" / f"fecha={fecha:%Y-%m-%d}"
    carpeta.mkdir(parents=True, exist_ok=True)
    por_hora = defaultdict(list)
    for ev in sorted(eventos, key=lambda e: e["received_at"]):
        h = datetime.fromisoformat(ev["received_at"]).astimezone(LIMA).hour
        por_hora[h].append(ev)
    archivos, corruptos = [], 0
    for h, evs in sorted(por_hora.items()):
        ruta = carpeta / f"eventos_{fecha:%Y-%m-%d}_{h:02d}.jsonl"
        with ruta.open("w", encoding="utf-8") as f:
            for ev in evs:
                linea = json.dumps(ev, ensure_ascii=False)
                if rng.random() < P_CORRUPTO:                 # caso borde: línea truncada
                    linea = linea[: rng.randint(20, len(linea) - 5)]; corruptos += 1
                f.write(linea + "\n")
        archivos.append(ruta)
    return archivos, corruptos


# =============================================================================
# 5. PUNTO DE ENTRADA
# =============================================================================
def main() -> None:
    ap = argparse.ArgumentParser(description="Genera una muestra .jsonl del clickstream de la app de Andina Market.")
    ap.add_argument("--fecha", default=datetime.now(LIMA).strftime("%Y-%m-%d"), help="Día (hora de Lima). Por defecto hoy.")
    ap.add_argument("--sesiones", type=int, default=2000, help="Sesiones de navegación sin compra (además de las compras reales).")
    ap.add_argument("--semilla", type=int, default=None, help="Por defecto aleatoria (cada corrida es distinta).")
    ap.add_argument("--sin-db", action="store_true", help="No conectarse a Azure SQL: IDs por rango y sin compras reales.")
    ap.add_argument("--salida", default=str(Path(__file__).resolve().parent / "salida"),
                    help="Carpeta local de salida (por defecto fuentes/salida/, está en .gitignore).")
    args = ap.parse_args()

    fecha = date.fromisoformat(args.fecha)
    semilla = args.semilla if args.semilla is not None else int(time.time())
    rng = random.Random(semilla); random.seed(semilla)

    ref = cargar_referencia(fecha, args.sin_db)
    g = Generador(ref, fecha, rng)
    for pedido in ref["pedidos_app"]:
        g.sesion_con_compra(pedido)
    for _ in range(args.sesiones):
        g.sesion_navegacion()
    g.aplicar_casos_borde()
    archivos, corruptos = escribir(g.eventos, fecha, Path(args.salida), rng)
    g.casos["lineas_json_corruptas"] = corruptos

    resumen = {"fecha": args.fecha, "semilla": semilla, "origen_ids": "rango (--sin-db)" if args.sin_db else "Azure SQL",
               "eventos": len(g.eventos), "archivos": len(archivos),
               "por_tipo": dict(Counter(e["event_type"] for e in g.eventos)),
               "casos_borde": dict(g.casos),
               "carpeta": str(archivos[0].parent) if archivos else None}
    print(json.dumps(resumen, indent=2, ensure_ascii=False))
    # Junto al script (fuentes/logs/), sin importar desde qué carpeta se ejecute.
    carpeta_logs = Path(__file__).resolve().parent / "logs"
    carpeta_logs.mkdir(exist_ok=True)
    log = carpeta_logs / f"clickstream_{args.fecha}_{datetime.now():%H%M%S}.json"
    log.write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Resumen guardado en {log}")


if __name__ == "__main__":
    main()
