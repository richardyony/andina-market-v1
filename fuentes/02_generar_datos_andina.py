#!/usr/bin/env python3
"""
===============================================================================
 Archivo      : 02_generar_datos_andina.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Fuente (OLTP Azure SQL) · paso 02 de 03
 Propósito    : Generar datos sintéticos realistas e INYECTARLOS directamente en
                la base de origen (sin archivo intermedio, como pide el reto),
                y simular la operación diaria para probar la ingesta con CDC.

 Modos
   inicial    : Carga histórica de ~18 meses. Se ejecuta UNA vez, ANTES de
                habilitar CDC (03_habilitar_cdc.sql). Inserta clientes,
                productos, pedidos, líneas, pagos y tickets coherentes entre sí
                y con casos borde sembrados a propósito.
   dia        : Simula un día de operación DESPUÉS de habilitar CDC: pedidos
                nuevos, pagos que cambian de estado (incluso dos veces en el
                mismo día), clientes que cambian de segmento, precios que
                cambian, pedidos que avanzan, tickets y algunos DELETE.
                Imprime y guarda un resumen para verificar la ingesta.

 Uso
   python 02_generar_datos_andina.py --modo inicial --dry-run   # solo genera y muestra estadísticas
   python 02_generar_datos_andina.py --modo inicial             # inserta en Azure SQL
   python 02_generar_datos_andina.py --modo inicial --reset     # borra y recarga (solo sin CDC)
   python 02_generar_datos_andina.py --modo dia                 # simula un día de cambios

 Conexión (variables de entorno; NUNCA credenciales en el código ni en Git)
   ANDINA_SQL_SERVER    sql-andinamarket-cus-001.database.windows.net (por defecto)
   ANDINA_SQL_DB        sqldb-andinamarket-cus-001 (por defecto)
   ANDINA_SQL_USER      usuario con permisos de escritura (p. ej. sqladmin_andina)
   ANDINA_SQL_PASSWORD  contraseña de ese usuario

 Entradas     : parámetros de línea de comandos (ver --help)
 Salidas      : filas en dbo.* ; resumen en consola ; logs/<modo>_<fecha>.json
 Dependencias : Python 3.10+, pyodbc, ODBC Driver 18 for SQL Server
 Reproducible : semilla fija (--semilla). Misma semilla + misma fecha = mismos datos.
 Autoría      : Richard Huamán · construido con asistencia de IA (Claude);
                los prompts usados se documentan en el README del repositorio.
 Versión      : 1.0 · 2026-10-10
===============================================================================
"""

from __future__ import annotations

import argparse
import bisect
import json
import os
import random
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

# pyodbc solo es obligatorio cuando se escribe en la base (no en --dry-run).
try:
    import pyodbc  # type: ignore
except ImportError:  # pragma: no cover
    pyodbc = None


# =============================================================================
# 1. CONFIGURACIÓN · volúmenes y proporciones
#    Todos los porcentajes de casos borde están aquí para que sean fáciles de
#    revisar, justificar en el README y ajustar sin tocar la lógica.
# =============================================================================
@dataclass
class Volumen:
    """Volumen de la carga inicial (modo inicial)."""
    clientes: int = 5000
    productos: int = 400
    pedidos: int = 40000
    tickets: int = 6000
    meses_historia: int = 18


@dataclass
class VolumenDia:
    """Cantidad de eventos que simula una ejecución del modo dia."""
    clientes_nuevos: int = 20
    pedidos_nuevos: int = 150
    pagos_a_procesar: int = 180
    pedidos_a_enviar: int = 120
    pedidos_a_entregar: int = 120
    cambios_segmento: int = 30
    correcciones_cliente: int = 10
    cambios_precio: int = 10
    tickets_nuevos: int = 25
    tickets_a_avanzar: int = 40
    lineas_a_eliminar: int = 5


# Casos borde de la carga inicial (proporción sobre el total de filas)
P_TELEFONO_NULO = 0.02        # clientes sin teléfono
P_CIUDAD_NULA = 0.03          # clientes sin ciudad
P_CIUDAD_SUCIA = 0.05         # 'LIMA', 'lima ', 'Lima Metropolitana'...
P_EMAIL_SUCIO = 0.04          # mayúsculas o espacios alrededor
P_CLIENTE_DUPLICADO = 0.015   # misma persona registrada dos veces
P_TOTAL_DESCUADRADO = 0.003   # bug "legacy": total ≠ suma de líneas
P_PEDIDO_SIN_LINEAS = 0.001   # pedido cancelado sin detalle
P_TICKET_BODY_NULO = 0.02
P_TICKET_LARGO = 0.01
P_TICKET_EMOJI = 0.02

# Escenarios de pago para cada pedido (deben sumar 1.0)
ESCENARIOS_PAGO = {
    "aprobado_1er_intento": 0.82,
    "rechazado_luego_aprobado": 0.09,   # 2 filas en Payments
    "rechazado_definitivo": 0.03,       # pedido cancelado
    "sin_pago": 0.03,                   # carrito abandonado
    "aprobado_luego_reembolsado": 0.03, # cancelación con devolución de dinero
}


# =============================================================================
# 2. CATÁLOGOS DE REFERENCIA · nombres, ciudades, categorías
#    Listas propias (sin dependencias como Faker) para que los nombres y
#    ciudades sean de la región andina y el script sea fácil de instalar.
# =============================================================================
NOMBRES_F = ["María", "Ana", "Lucía", "Rosa", "Carmen", "Sofía", "Valeria", "Camila",
             "Daniela", "Gabriela", "Fernanda", "Andrea", "Paola", "Milagros", "Yesenia",
             "Ximena", "Natalia", "Carolina", "Juana", "Flor", "Isabel", "Mariana", "Luz"]
NOMBRES_M = ["José", "Luis", "Carlos", "Jorge", "Juan", "Miguel", "Diego", "Andrés",
             "Javier", "Ricardo", "Fernando", "Pedro", "Raúl", "Óscar", "Hugo", "Iván",
             "Sebastián", "Mateo", "Santiago", "Alejandro", "Wilmer", "Edwin", "Néstor"]
APELLIDOS = ["Quispe", "Mamani", "Huamán", "Flores", "García", "Rodríguez", "Torres",
             "Ramírez", "Chávez", "Vargas", "Rojas", "Castillo", "Gutiérrez", "Mendoza",
             "Sánchez", "Díaz", "Gómez", "López", "Pérez", "Martínez", "Condori", "Choque",
             "Villca", "Ñahui", "Muñoz", "Peña", "Salazar", "Cruz", "Morales", "Núñez",
             "Paredes", "Zapata", "Restrepo", "Ospina", "Cevallos", "Andrade", "Zambrano",
             "Cárdenas", "Ríos", "Aguilar", "Herrera", "Medina", "Espinoza", "Álvarez"]

# País → (peso de clientes, offset UTC aproximado, prefijo telefónico, ciudades con peso)
PAISES = {
    "PE": (0.45, -5, "+51 9", [("Lima", 55), ("Arequipa", 10), ("Trujillo", 8), ("Cusco", 7),
                               ("Piura", 6), ("Chiclayo", 6), ("Huancayo", 4), ("Iquitos", 4)]),
    "CO": (0.25, -5, "+57 3", [("Bogotá", 40), ("Medellín", 22), ("Cali", 15),
                               ("Barranquilla", 10), ("Cartagena", 7), ("Bucaramanga", 6)]),
    "EC": (0.12, -5, "+593 9", [("Quito", 42), ("Guayaquil", 40), ("Cuenca", 12), ("Manta", 6)]),
    "BO": (0.08, -4, "+591 7", [("Santa Cruz de la Sierra", 35), ("La Paz", 30), ("Cochabamba", 18),
                                ("El Alto", 10), ("Sucre", 7)]),
    "CL": (0.10, -3, "+56 9", [("Santiago", 60), ("Valparaíso", 12), ("Concepción", 12),
                               ("Antofagasta", 9), ("Arica", 7)]),
}
VARIANTES_CIUDAD_SUCIA = {  # cómo escribe mal la gente su ciudad (caso borde)
    "Lima": ["LIMA", "lima ", "Lima Metropolitana", "Lima - Perú"],
    "Bogotá": ["Bogota", "BOGOTA D.C.", "bogotá"],
    "Santiago": ["Stgo", "SANTIAGO", "Santiago de Chile"],
    "Quito": ["QUITO", "quito "],
    "La Paz": ["LA PAZ", "La paz"],
}
DOMINIOS_EMAIL = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "icloud.com"]

# Categoría → (precio mín, precio máx, sustantivos, características para la descripción)
CATEGORIAS = {
    "Electrónica": (25, 1800, ["Audífonos inalámbricos", "Parlante bluetooth", "Smartwatch",
                               "Tablet", "Cargador rápido", "Teclado mecánico", "Mouse inalámbrico",
                               "Monitor", "Cámara web", "Power bank"],
                    ["batería de larga duración", "conexión bluetooth 5.3", "resistente al agua IPX4",
                     "carga rápida USB-C", "pantalla de alta resolución"]),
    "Hogar": (10, 900, ["Juego de sábanas", "Olla a presión", "Licuadora", "Cafetera",
                        "Juego de cuchillos", "Lámpara de mesa", "Aspiradora", "Freidora de aire"],
                       ["acero inoxidable", "fácil de limpiar", "bajo consumo eléctrico",
                        "diseño compacto", "apto para lavavajillas"]),
    "Moda": (12, 350, ["Polo de algodón", "Casaca impermeable", "Jean clásico", "Zapatillas urbanas",
                       "Chompa de alpaca", "Vestido casual", "Mochila", "Gorra"],
                      ["algodón pima", "lana de alpaca", "tallas de S a XL", "costuras reforzadas",
                       "material transpirable"]),
    "Deportes": (15, 1200, ["Bicicleta montañera", "Pelota de fútbol", "Mancuernas", "Mat de yoga",
                            "Zapatillas running", "Carpa 4 personas", "Botella térmica"],
                           ["uso profesional", "material antideslizante", "ultraligero",
                            "resistente a la intemperie", "garantía extendida"]),
    "Belleza": (8, 250, ["Crema hidratante", "Protector solar", "Perfume", "Shampoo sin sulfatos",
                         "Set de maquillaje", "Plancha de cabello"],
                        ["dermatológicamente probado", "ingredientes naturales", "libre de parabenos",
                         "protección UV", "apto para piel sensible"]),
    "Juguetes": (10, 400, ["Set de bloques", "Muñeca articulada", "Rompecabezas 1000 piezas",
                           "Auto a control remoto", "Juego de mesa"],
                          ["para mayores de 6 años", "materiales no tóxicos", "fomenta la creatividad",
                           "incluye baterías"]),
    "Supermercado": (3, 120, ["Café orgánico", "Quinua blanca", "Aceite de oliva", "Chocolate 70%",
                              "Pack de agua", "Mix de frutos secos"],
                             ["producto nacional", "certificado orgánico", "sin gluten",
                              "presentación familiar"]),
    "Libros": (8, 150, ["Novela", "Libro de cocina peruana", "Guía de viaje", "Cuento infantil",
                        "Manual de programación"],
                       ["tapa dura", "edición ilustrada", "best seller regional", "edición 2026"]),
}
MARCAS = ["Kallpa", "Inti", "Nevado", "Wayra", "Pacha", "Ayni", "Killa", "Urpi", "Sumaq", "Tika"]

CANALES = [("web", 45), ("app", 40), ("tienda", 15)]
METODOS_PAGO = [("tarjeta_credito", 38), ("tarjeta_debito", 22), ("yape", 15), ("plin", 7),
                ("transferencia", 10), ("contraentrega", 8)]

# Estacionalidad: factor por mes (Día de la Madre en mayo, Fiestas Patrias en julio,
# Cyber/Black Friday en noviembre, Navidad en diciembre)
FACTOR_MES = {1: 0.85, 2: 0.80, 3: 0.90, 4: 0.95, 5: 1.25, 6: 1.00,
              7: 1.15, 8: 0.95, 9: 0.95, 10: 1.00, 11: 1.60, 12: 1.75}
FACTOR_DIA_SEMANA = [1.05, 1.00, 1.00, 1.00, 1.10, 1.15, 0.85]  # lunes..domingo
PESO_HORA_LOCAL = [1, 1, 1, 1, 1, 1, 2, 4, 6, 7, 8, 9, 11, 11, 9, 8, 8, 9, 11, 13, 13, 11, 7, 3]

TICKETS_TIPOS = [  # (asunto, plantilla del cuerpo, prioridad)
    ("Mi pedido no llegó", "Hola, hice el pedido #{pedido} hace {dias} días y todavía no me llega. "
                           "El seguimiento no se actualiza. ¿Me pueden ayudar?", "alta"),
    ("Producto llegó dañado", "El {producto} del pedido #{pedido} llegó con la caja rota y no funciona. "
                              "Solicito cambio o devolución.", "alta"),
    ("Solicitud de devolución", "Quiero devolver el {producto} porque no es lo que esperaba. "
                                "¿Cuál es el procedimiento?", "media"),
    ("Problema con el pago", "Me cobraron dos veces el pedido #{pedido}. Adjunto el estado de cuenta.", "alta"),
    ("Consulta de stock", "¿Cuándo vuelve a haber stock del {producto}?", "baja"),
    ("Cambio de dirección", "Necesito cambiar la dirección de entrega del pedido #{pedido}.", "media"),
    ("Factura", "Necesito la factura del pedido #{pedido} a nombre de mi empresa.", "baja"),
]


# =============================================================================
# 3. UTILIDADES
# =============================================================================
def dinero(x: float) -> Decimal:
    """Redondea a 2 decimales como lo haría un sistema contable (half-up)."""
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def sin_tildes(s: str) -> str:
    """Quita tildes y ñ para construir emails válidos."""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)).replace("ñ", "n")


def elegir(pares, rng: random.Random):
    """Elige un valor de una lista [(valor, peso), ...]."""
    valores, pesos = zip(*pares)
    return rng.choices(valores, weights=pesos, k=1)[0]


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# =============================================================================
# 4. CONEXIÓN A AZURE SQL
#    La base es serverless con auto-pausa: el primer intento puede fallar
#    mientras "despierta" (error 40613). Por eso se reintenta con espera.
# =============================================================================
def conectar():
    if pyodbc is None:
        sys.exit("Falta pyodbc: pip install pyodbc  (y el ODBC Driver 18 for SQL Server)")
    servidor = os.getenv("ANDINA_SQL_SERVER", "sql-andinamarket-cus-001.database.windows.net")
    base = os.getenv("ANDINA_SQL_DB", "sqldb-andinamarket-cus-001")
    usuario = os.getenv("ANDINA_SQL_USER")
    clave = os.getenv("ANDINA_SQL_PASSWORD")
    if not usuario or not clave:
        sys.exit("Define ANDINA_SQL_USER y ANDINA_SQL_PASSWORD como variables de entorno.")
    cadena = (f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{servidor},1433;"
              f"Database={base};Uid={usuario};Pwd={clave};Encrypt=yes;"
              f"TrustServerCertificate=no;Connection Timeout=60;")
    for intento in range(1, 6):
        try:
            conn = pyodbc.connect(cadena, autocommit=False)
            log(f"Conectado a {base} como {usuario}")
            return conn
        except pyodbc.Error as e:  # base reanudándose, red, firewall...
            log(f"Intento {intento}/5 falló ({str(e)[:90]}...). Reintento en 20 s")
            time.sleep(20)
    sys.exit("No se pudo conectar. Revisa firewall (tu IP), usuario y que la base no esté pausada por cuota.")


def cdc_habilitado(cur) -> bool:
    cur.execute("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()")
    return bool(cur.fetchone()[0])


# =============================================================================
# 5. MODO INICIAL · generación en memoria
#    Se genera TODO en memoria con IDs explícitos (IDENTITY_INSERT) para poder
#    armar las relaciones (FK) y la coherencia antes de tocar la base.
# =============================================================================
class GeneradorInicial:
    def __init__(self, vol: Volumen, semilla: int, hasta: date):
        self.vol = vol
        self.rng = random.Random(semilla)
        self.hasta = datetime(hasta.year, hasta.month, hasta.day)  # medianoche UTC del día de corte
        self.desde = self.hasta - timedelta(days=int(vol.meses_historia * 30.4))
        self.clientes, self.productos, self.pedidos = [], [], []
        self.lineas, self.pagos, self.tickets = [], [], []
        self.stats = defaultdict(int)  # contador de casos borde para el resumen

    # ---------------------------------------------------------------- clientes
    def generar_clientes(self) -> None:
        """Clientes con alta distribuida en ~30 meses; incluye nulos, texto sucio y duplicados."""
        rng = self.rng
        paises = list(PAISES.keys())
        pesos_pais = [PAISES[p][0] for p in paises]
        inicio_altas = self.hasta - timedelta(days=900)
        for cid in range(1, self.vol.clientes + 1):
            pais = rng.choices(paises, pesos_pais)[0]
            _, _, pref_tel, ciudades = PAISES[pais]
            nombre = rng.choice(NOMBRES_F + NOMBRES_M)
            apellido = f"{rng.choice(APELLIDOS)} {rng.choice(APELLIDOS)}"
            ciudad = elegir(ciudades, rng)
            email = (f"{sin_tildes(nombre).lower()}.{sin_tildes(apellido.split()[0]).lower()}"
                     f"{rng.randint(1, 999)}@{rng.choice(DOMINIOS_EMAIL)}")
            telefono = pref_tel + "".join(str(rng.randint(0, 9)) for _ in range(8))
            alta = inicio_altas + timedelta(days=rng.randint(0, 897), minutes=rng.randint(0, 1439))

            # --- casos borde -------------------------------------------------
            if rng.random() < P_TELEFONO_NULO:
                telefono = None; self.stats["cliente_telefono_nulo"] += 1
            if rng.random() < P_CIUDAD_NULA:
                ciudad = None; self.stats["cliente_ciudad_nula"] += 1
            elif ciudad in VARIANTES_CIUDAD_SUCIA and rng.random() < P_CIUDAD_SUCIA * 2:
                ciudad = rng.choice(VARIANTES_CIUDAD_SUCIA[ciudad]); self.stats["cliente_ciudad_sucia"] += 1
            if rng.random() < P_EMAIL_SUCIO:
                email = rng.choice([email.upper(), f" {email}", f"{email} "]); self.stats["cliente_email_sucio"] += 1

            self.clientes.append(dict(CustomerID=cid, FirstName=nombre, LastName=apellido, Email=email,
                                      Phone=telefono, City=ciudad, CountryCode=pais, Segment="nuevo",
                                      SignupDate=alta.date(), IsActive=1, CreatedAt=alta, UpdatedAt=alta,
                                      _peso=min(rng.paretovariate(1.3), 40.0)))

        # Duplicados de negocio: la misma persona vuelve a registrarse con el email
        # escrito distinto. Silver deberá detectarlos (no basta con la PK).
        n_dup = int(self.vol.clientes * P_CLIENTE_DUPLICADO)
        for i in range(n_dup):
            orig = rng.choice(self.clientes[: self.vol.clientes])
            alta = datetime.combine(orig["SignupDate"], datetime.min.time()) + timedelta(days=rng.randint(30, 200))
            if alta >= self.hasta:
                alta = self.hasta - timedelta(days=2)
            email = rng.choice([orig["Email"].strip().upper(), orig["Email"].strip() + " ",
                                orig["Email"].strip().replace(".", "", 1)])
            dup = dict(orig, CustomerID=self.vol.clientes + i + 1, Email=email, SignupDate=alta.date(),
                       CreatedAt=alta, UpdatedAt=alta, _peso=0.5)
            self.clientes.append(dup)
            self.stats["cliente_duplicado"] += 1

        # IDs crecientes según la fecha de alta, como los asignaría el IDENTITY real
        self.clientes.sort(key=lambda c: c["CreatedAt"])
        for nuevo_id, c in enumerate(self.clientes, start=1):
            c["CustomerID"] = nuevo_id

    # --------------------------------------------------------------- productos
    def generar_productos(self) -> None:
        """Catálogo con precio por categoría; algunos agotados o descontinuados."""
        rng = self.rng
        cats = list(CATEGORIAS.keys())
        for pid in range(1, self.vol.productos + 1):
            cat = cats[(pid - 1) % len(cats)] if pid <= len(cats) * 3 else rng.choice(cats)
            pmin, pmax, sustantivos, rasgos = CATEGORIAS[cat]
            nombre = f"{rng.choice(sustantivos)} {rng.choice(MARCAS)} {rng.choice(['', 'Pro', 'Plus', 'Lite', 'Max', '2.0'])}".strip()
            # Distribución sesgada a precios bajos (la mayoría del catálogo es barato, pocos productos caros)
            precio = dinero(round(pmin * (pmax / pmin) ** (rng.random() ** 2.2)) - 0.10)
            r1, r2 = rng.sample(rasgos, 2)
            descripcion = (f"{nombre}. Producto de la categoría {cat.lower()} con {r1} y {r2}. "
                           f"Garantía de {rng.choice([3, 6, 12, 24])} meses. Envío a todo el país.")
            estado = rng.choices(["activo", "agotado", "descontinuado"], [92, 3, 5])[0]
            creado = self.desde - timedelta(days=rng.randint(1, 400))
            self.productos.append(dict(ProductID=pid, SKU=f"{sin_tildes(cat)[:3].upper()}-{pid:05d}",
                                       ProductName=nombre, Category=cat, Price=precio,
                                       Description=descripcion if rng.random() > 0.03 else None,
                                       Status=estado, CreatedAt=creado, UpdatedAt=creado))
        self.productos.sort(key=lambda p: p["CreatedAt"])
        for nuevo_id, p in enumerate(self.productos, start=1):
            p["ProductID"] = nuevo_id
            p["SKU"] = f"{p['SKU'][:3]}-{nuevo_id:05d}"
        self.stats["producto_sin_descripcion"] = sum(1 for p in self.productos if p["Description"] is None)

    # ---------------------------------------------------- calendario de ventas
    def _preparar_calendario(self) -> None:
        """Pesos acumulados por día (mes, día de semana, Black Friday) para muestrear fechas."""
        self.dias, acumulado, total = [], [], 0.0
        d = self.desde
        while d < self.hasta:
            peso = FACTOR_MES[d.month] * FACTOR_DIA_SEMANA[d.weekday()]
            if d.month == 11 and d.weekday() == 4 and 22 <= d.day <= 28:  # Black Friday
                peso *= 3.0
            total += peso
            self.dias.append(d)
            acumulado.append(total)
            d += timedelta(days=1)
        self.acumulado = acumulado

    def _fecha_pedido(self, no_antes_de: datetime, offset_utc: int) -> datetime:
        """Elige un día respetando la estacionalidad y una hora local realista; devuelve UTC."""
        i0 = max(0, bisect.bisect_left(self.dias, no_antes_de))
        base = self.acumulado[i0 - 1] if i0 > 0 else 0.0
        x = self.rng.uniform(base, self.acumulado[-1])
        dia = self.dias[min(bisect.bisect_left(self.acumulado, x), len(self.dias) - 1)]
        hora = self.rng.choices(range(24), PESO_HORA_LOCAL)[0]
        local = dia + timedelta(hours=hora, minutes=self.rng.randint(0, 59), seconds=self.rng.randint(0, 59))
        return local - timedelta(hours=offset_utc)  # hora local → UTC

    # ----------------------------------------- pedidos, líneas y pagos
    def generar_pedidos(self) -> None:
        """Pedidos coherentes: líneas con precio histórico, total que cuadra, pagos y estado consistentes."""
        rng = self.rng
        self._preparar_calendario()
        activos_historicos = self.productos  # descontinuados también aparecen en pedidos antiguos
        asignados = rng.choices(self.clientes, weights=[c["_peso"] for c in self.clientes], k=self.vol.pedidos)
        # Primero se fija la fecha de cada pedido y se ordena cronológicamente:
        # así OrderID crece con OrderDate, como en un sistema real.
        agenda = []
        for cli in asignados:
            alta = datetime.combine(cli["SignupDate"], datetime.min.time())
            t0 = self._fecha_pedido(max(alta, self.desde), PAISES[cli["CountryCode"]][1])
            if t0 >= self.hasta - timedelta(hours=2):  # margen para que pagos/actualizaciones no pasen del corte
                t0 = self.hasta - timedelta(hours=rng.randint(2, 20))
            agenda.append((t0, cli))
        agenda.sort(key=lambda x: x[0])
        pid_pago = 0
        id_linea = 0
        for oid, (t0, cli) in enumerate(agenda, start=1):
            canal = elegir(CANALES, rng)
            edad_dias = (self.hasta - t0).total_seconds() / 86400

            # --- líneas ---------------------------------------------------
            lineas = []
            if rng.random() < P_PEDIDO_SIN_LINEAS:
                self.stats["pedido_sin_lineas"] += 1
            else:
                n = rng.choices([1, 2, 3, 4, 5], [45, 28, 15, 8, 4])[0]
                for prod in rng.sample(activos_historicos, n):
                    # Precio histórico: el precio actual "deriva" hacia atrás en el tiempo
                    deriva = 1 - rng.uniform(0, 0.12) * (edad_dias / 548)
                    precio = dinero(float(prod["Price"]) * deriva)
                    cant = rng.choices([1, 2, 3, 4], [70, 20, 7, 3])[0]
                    desc = dinero(float(precio) * cant * rng.choice([0.05, 0.10, 0.15, 0.20])) if rng.random() < 0.2 else Decimal("0.00")
                    id_linea += 1
                    lineas.append(dict(OrderItemID=id_linea, OrderID=oid, ProductID=prod["ProductID"],
                                       Quantity=cant, UnitPrice=precio, DiscountAmount=desc,
                                       CreatedAt=t0, UpdatedAt=t0))
            total = sum((l["UnitPrice"] * l["Quantity"] - l["DiscountAmount"] for l in lineas), Decimal("0.00"))
            if lineas and rng.random() < P_TOTAL_DESCUADRADO:
                total += dinero(rng.uniform(0.01, 5.0)); self.stats["pedido_total_descuadrado"] += 1

            # --- pagos y estado del pedido -------------------------------
            escenario = "sin_pago" if not lineas else rng.choices(list(ESCENARIOS_PAGO), list(ESCENARIOS_PAGO.values()))[0]
            metodo = elegir(METODOS_PAGO, rng) if canal != "tienda" else rng.choice(["tarjeta_credito", "tarjeta_debito", "yape", "plin"])
            if metodo == "contraentrega" and escenario.startswith("rechazado"):
                escenario = "aprobado_1er_intento"  # el pago contra entrega no se rechaza en línea
            pagos_pedido, estado, actualizado = [], "pendiente", t0

            def nuevo_pago(estado_pago, cuando, cerrado, met=metodo):
                nonlocal pid_pago
                pid_pago += 1
                pagos_pedido.append(dict(PaymentID=pid_pago, OrderID=oid, PaymentMethod=met, Amount=total,
                                         Status=estado_pago, PaymentDate=cuando,
                                         TransactionRef=f"TX{pid_pago:08d}{rng.randint(100, 999)}" if met != "contraentrega" else None,
                                         CreatedAt=cuando, UpdatedAt=cerrado))

            t_pago = t0 + timedelta(minutes=rng.randint(1, 30))
            if escenario == "sin_pago":
                estado = "cancelado" if edad_dias > 2 else "pendiente"
                actualizado = t0 + timedelta(days=2) if estado == "cancelado" else t0
            elif escenario == "rechazado_definitivo":
                nuevo_pago("rechazado", t_pago, t_pago + timedelta(seconds=40))
                estado, actualizado = "cancelado", t_pago + timedelta(hours=1)
            else:
                if escenario == "rechazado_luego_aprobado":
                    nuevo_pago("rechazado", t_pago, t_pago + timedelta(seconds=35))
                    t_pago += timedelta(minutes=rng.randint(2, 50))
                    metodo = elegir(METODOS_PAGO[:5], rng)  # reintenta con otro medio
                if metodo == "contraentrega" and edad_dias < 4:
                    nuevo_pago("pendiente", t_pago, t_pago)  # se paga al entregar
                    estado = "enviado" if edad_dias > 1 else "pendiente"
                    actualizado = t0 + timedelta(days=1) if estado == "enviado" else t0
                else:
                    t_ok = t_pago + timedelta(seconds=rng.randint(5, 60))
                    nuevo_pago("aprobado", t_pago, t_ok, metodo)
                    estado, actualizado = "pagado", t_ok
                    if canal == "tienda":
                        estado = "entregado"
                    elif edad_dias > 7:
                        estado, actualizado = "entregado", t0 + timedelta(days=rng.randint(2, 6))
                    elif edad_dias > 2:
                        estado, actualizado = rng.choice(["enviado", "entregado"]), t0 + timedelta(days=2)
                    # devoluciones o cancelaciones con reembolso
                    if escenario == "aprobado_luego_reembolsado" and edad_dias > 3:
                        t_r = actualizado + timedelta(days=rng.randint(1, 10))
                        if t_r < self.hasta:
                            pagos_pedido[-1]["Status"], pagos_pedido[-1]["UpdatedAt"] = "reembolsado", t_r
                            estado = "devuelto" if estado == "entregado" else "cancelado"
                            actualizado = t_r
            actualizado = min(actualizado, self.hasta - timedelta(seconds=1))
            for p in pagos_pedido:
                p["UpdatedAt"] = min(p["UpdatedAt"], self.hasta - timedelta(seconds=1))
            if len(pagos_pedido) > 1:
                self.stats["pedido_con_varios_intentos_pago"] += 1

            self.pedidos.append(dict(OrderID=oid, CustomerID=cli["CustomerID"], OrderDate=t0, Channel=canal,
                                     Status=estado, TotalAmount=total, CreatedAt=t0, UpdatedAt=actualizado))
            self.lineas.extend(lineas)
            self.pagos.extend(pagos_pedido)

        # Segmento coherente con el comportamiento de compra al cierre de la historia:
        # vip = top 5 % por gasto, frecuente = siguiente 15 %, regular = 2+ compras, nuevo = resto
        compras = defaultdict(lambda: [0, Decimal("0")])
        for o in self.pedidos:
            if o["Status"] not in ("cancelado",):
                compras[o["CustomerID"]][0] += 1
                compras[o["CustomerID"]][1] += o["TotalAmount"]
        ranking = sorted(self.clientes, key=lambda c: compras[c["CustomerID"]][1], reverse=True)
        n_vip, n_frec = int(len(ranking) * 0.05), int(len(ranking) * 0.15)
        for pos, c in enumerate(ranking):
            n = compras[c["CustomerID"]][0]
            c["Segment"] = ("vip" if pos < n_vip else "frecuente" if pos < n_vip + n_frec
                            else "regular" if n >= 2 else "nuevo")

    # ----------------------------------------------------------------- tickets
    def generar_tickets(self) -> None:
        """Tickets de soporte con texto libre; ~70 % ligados a un pedido del mismo cliente."""
        rng = self.rng
        pedidos_por_cliente = defaultdict(list)
        for o in self.pedidos:
            pedidos_por_cliente[o["CustomerID"]].append(o)
        clientes_con_pedidos = list(pedidos_por_cliente.keys())
        nombre_producto = {p["ProductID"]: p["ProductName"] for p in self.productos}
        producto_de_pedido = {}
        for l in self.lineas:
            producto_de_pedido.setdefault(l["OrderID"], l["ProductID"])
        for tid in range(1, self.vol.tickets + 1):
            cid = rng.choice(clientes_con_pedidos)
            pedido = rng.choice(pedidos_por_cliente[cid]) if rng.random() < 0.7 else None
            asunto, plantilla, prioridad = rng.choice(TICKETS_TIPOS)
            base_t = pedido["OrderDate"] if pedido else self.desde + timedelta(days=rng.randint(0, 540))
            abierto = min(base_t + timedelta(days=rng.randint(0, 12), hours=rng.randint(0, 23)), self.hasta - timedelta(hours=1))
            prod = nombre_producto.get(producto_de_pedido.get(pedido["OrderID"]) if pedido else rng.randint(1, len(self.productos)), "producto")
            cuerpo = plantilla.format(pedido=pedido["OrderID"] if pedido else "—", dias=rng.randint(3, 15), producto=prod)
            # --- casos borde -------------------------------------------------
            r = rng.random()
            if r < P_TICKET_BODY_NULO:
                cuerpo = None; self.stats["ticket_body_nulo"] += 1
            elif r < P_TICKET_BODY_NULO + P_TICKET_LARGO:
                cuerpo = (cuerpo + " ") * 25; self.stats["ticket_muy_largo"] += 1
            elif r < P_TICKET_BODY_NULO + P_TICKET_LARGO + P_TICKET_EMOJI:
                cuerpo += " 😡😡 ¡¡Urgente!!"; self.stats["ticket_con_emojis"] += 1
            if rng.random() < 0.03:
                asunto = asunto.upper()
            edad = (self.hasta - abierto).days
            estado = ("cerrado" if edad > 20 else rng.choice(["resuelto", "cerrado"]) if edad > 7
                      else rng.choice(["abierto", "en_proceso", "resuelto"]))
            cerrado_en = (abierto + timedelta(days=rng.randint(1, 6))) if estado in ("resuelto", "cerrado") else None
            if cerrado_en and cerrado_en >= self.hasta:
                cerrado_en = self.hasta - timedelta(minutes=5)
            self.tickets.append(dict(TicketID=tid, CustomerID=cid, OrderID=pedido["OrderID"] if pedido else None,
                                     Subject=asunto, Body=cuerpo, Priority=prioridad, Status=estado,
                                     OpenedAt=abierto, ClosedAt=cerrado_en, CreatedAt=abierto,
                                     UpdatedAt=cerrado_en or abierto))

    def _ordenar_tickets(self) -> None:
        """TicketID creciente según la fecha de apertura."""
        self.tickets.sort(key=lambda t: t["OpenedAt"])
        for nuevo_id, t in enumerate(self.tickets, start=1):
            t["TicketID"] = nuevo_id

    def generar_todo(self) -> None:
        log(f"Generando historia {self.desde:%Y-%m-%d} → {self.hasta:%Y-%m-%d}")
        self.generar_clientes(); self.generar_productos()
        self.generar_pedidos(); self.generar_tickets(); self._ordenar_tickets()

    def autocontrol(self) -> dict:
        """Reglas de coherencia verificadas en memoria (deben dar 0 salvo los casos borde sembrados)."""
        alta = {c["CustomerID"]: c["SignupDate"] for c in self.clientes}
        fecha_pedido = {o["OrderID"]: o["OrderDate"] for o in self.pedidos}
        todas = self.clientes + self.productos + self.pedidos + self.lineas + self.pagos + self.tickets
        return {
            "pagos_antes_del_pedido": sum(p["PaymentDate"] < fecha_pedido[p["OrderID"]] for p in self.pagos),
            "pedidos_antes_del_alta": sum(o["OrderDate"].date() < alta[o["CustomerID"]] for o in self.pedidos),
            "updated_menor_que_created": sum(r["UpdatedAt"] < r["CreatedAt"] for r in todas),
            "fechas_futuras": sum(r["UpdatedAt"] >= self.hasta for r in todas),
            "tickets_cierre_antes_apertura": sum(1 for t in self.tickets if t["ClosedAt"] and t["ClosedAt"] < t["OpenedAt"]),
            "ticket_promedio_usd": float(sum(o["TotalAmount"] for o in self.pedidos) / max(len(self.pedidos), 1)),
        }

    def resumen(self) -> dict:
        estados = defaultdict(int)
        for o in self.pedidos:
            estados[o["Status"]] += 1
        estados_pago = defaultdict(int)
        for p in self.pagos:
            estados_pago[p["Status"]] += 1
        segmentos = defaultdict(int)
        for c in self.clientes:
            segmentos[c["Segment"]] += 1
        return {"filas": {"Customers": len(self.clientes), "Products": len(self.productos),
                          "Orders": len(self.pedidos), "OrderItems": len(self.lineas),
                          "Payments": len(self.pagos), "SupportTickets": len(self.tickets)},
                "estado_pedidos": dict(estados), "estado_pagos": dict(estados_pago),
                "segmentos": dict(segmentos), "casos_borde": dict(self.stats),
                "autocontrol": self.autocontrol()}


# =============================================================================
# 6. MODO INICIAL · escritura en Azure SQL
# =============================================================================
COLUMNAS = {  # orden de inserción = orden de las FK (padres antes que hijos)
    "Customers": ["CustomerID", "FirstName", "LastName", "Email", "Phone", "City", "CountryCode",
                  "Segment", "SignupDate", "IsActive", "CreatedAt", "UpdatedAt"],
    "Products": ["ProductID", "SKU", "ProductName", "Category", "Price", "Description", "Status",
                 "CreatedAt", "UpdatedAt"],
    "Orders": ["OrderID", "CustomerID", "OrderDate", "Channel", "Status", "TotalAmount", "CreatedAt", "UpdatedAt"],
    "OrderItems": ["OrderItemID", "OrderID", "ProductID", "Quantity", "UnitPrice", "DiscountAmount",
                   "CreatedAt", "UpdatedAt"],
    "Payments": ["PaymentID", "OrderID", "PaymentMethod", "Amount", "Status", "PaymentDate",
                 "TransactionRef", "CreatedAt", "UpdatedAt"],
    "SupportTickets": ["TicketID", "CustomerID", "OrderID", "Subject", "Body", "Priority", "Status",
                       "OpenedAt", "ClosedAt", "CreatedAt", "UpdatedAt"],
}
COLUMNAS_MAX = {"Description", "Body"}  # NVARCHAR(MAX): requieren setinputsizes con fast_executemany


def insertar_tabla(conn, tabla: str, filas: list[dict], lote: int = 1000) -> None:
    """Inserta por lotes con IDs explícitos. Una transacción por tabla: o entra completa o nada."""
    cols = COLUMNAS[tabla]
    sql = f"INSERT INTO dbo.{tabla} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
    cur = conn.cursor()
    cur.fast_executemany = True
    tamanos = [(pyodbc.SQL_WVARCHAR, 0, 0) if c in COLUMNAS_MAX else None for c in cols]
    try:
        cur.execute(f"SET IDENTITY_INSERT dbo.{tabla} ON")
        for i in range(0, len(filas), lote):
            if any(tamanos):
                try:
                    cur.setinputsizes(tamanos)  # evita buffers enormes en columnas NVARCHAR(MAX)
                except Exception as e:  # versiones antiguas de pyodbc no aceptan None en la lista
                    log(f"  (setinputsizes no aplicado: {e})")
                    tamanos = [None] * len(cols)
            cur.executemany(sql, [tuple(f[c] for c in cols) for f in filas[i:i + lote]])
        cur.execute(f"SET IDENTITY_INSERT dbo.{tabla} OFF")
        conn.commit()
        log(f"  {tabla:<15} {len(filas):>7,} filas")
    except Exception:
        conn.rollback()
        raise


def resetear(conn) -> None:
    """Vacía las tablas (hijos primero) y reinicia los IDENTITY. Bloqueado si CDC está activo."""
    cur = conn.cursor()
    if cdc_habilitado(cur):
        sys.exit("CDC está habilitado: --reset llenaría las tablas de cambios con DELETE. "
                 "Deshabilita CDC primero (sys.sp_cdc_disable_db) si de verdad quieres recargar.")
    for t in ["SupportTickets", "Payments", "OrderItems", "Orders", "Products", "Customers"]:
        cur.execute(f"DELETE FROM dbo.{t}")
        cur.execute(f"DBCC CHECKIDENT ('dbo.{t}', RESEED, 0)")
    conn.commit()
    log("Tablas vaciadas e IDENTITY reiniciados")


def validar_origen(conn) -> dict:
    """Consultas de control post-carga: conteos y reglas de coherencia."""
    consultas = {
        "pedidos_sin_lineas": "SELECT COUNT(*) FROM dbo.Orders o WHERE NOT EXISTS (SELECT 1 FROM dbo.OrderItems i WHERE i.OrderID = o.OrderID)",
        "pedidos_total_descuadrado": """SELECT COUNT(*) FROM dbo.Orders o
            JOIN (SELECT OrderID, SUM(Quantity*UnitPrice - DiscountAmount) s FROM dbo.OrderItems GROUP BY OrderID) i
              ON i.OrderID = o.OrderID WHERE i.s <> o.TotalAmount""",
        "pagos_antes_del_pedido": "SELECT COUNT(*) FROM dbo.Payments p JOIN dbo.Orders o ON o.OrderID = p.OrderID WHERE p.PaymentDate < o.OrderDate",
        "pedidos_antes_del_alta": "SELECT COUNT(*) FROM dbo.Orders o JOIN dbo.Customers c ON c.CustomerID = o.CustomerID WHERE CAST(o.OrderDate AS DATE) < c.SignupDate",
        "emails_repetidos_normalizados": """SELECT COUNT(*) FROM (SELECT LOWER(LTRIM(RTRIM(Email))) e FROM dbo.Customers
            GROUP BY LOWER(LTRIM(RTRIM(Email))) HAVING COUNT(*) > 1) x""",
    }
    cur = conn.cursor()
    res = {}
    for nombre, q in consultas.items():
        cur.execute(q)
        res[nombre] = cur.fetchone()[0]
    return res


def modo_inicial(args) -> dict:
    gen = GeneradorInicial(Volumen(clientes=args.clientes, pedidos=args.pedidos), args.semilla,
                           date.fromisoformat(args.hasta) if args.hasta else date.today())
    gen.generar_todo()
    resumen = gen.resumen()
    if args.dry_run:
        log("Dry-run: no se escribió nada en la base")
        return resumen

    conn = conectar()
    cur = conn.cursor()
    if args.reset:
        resetear(conn)
    cur.execute("SELECT COUNT(*) FROM dbo.Customers")
    if cur.fetchone()[0] > 0:
        sys.exit("dbo.Customers ya tiene datos. Usa --reset para recargar (solo si CDC no está activo).")
    if cdc_habilitado(cur):
        log("AVISO: CDC ya está activo; la carga inicial quedará registrada en las tablas de cambios.")

    log("Insertando (padres antes que hijos)...")
    for tabla, filas in [("Customers", gen.clientes), ("Products", gen.productos), ("Orders", gen.pedidos),
                         ("OrderItems", gen.lineas), ("Payments", gen.pagos), ("SupportTickets", gen.tickets)]:
        insertar_tabla(conn, tabla, filas)
    resumen["validaciones_en_base"] = validar_origen(conn)
    conn.close()
    return resumen


# =============================================================================
# 7. MODO DIA · simulador de operación diaria (después de habilitar CDC)
#    Cada evento se confirma en su propia transacción, como lo haría la app.
#    Todos los UPDATE envían UpdatedAt para que el trigger no genere un
#    segundo evento en CDC.
# =============================================================================
class SimuladorDia:
    SEGMENTOS = ["nuevo", "regular", "frecuente", "vip"]

    def __init__(self, conn, vol: VolumenDia, semilla: int):
        self.conn = conn
        self.cur = conn.cursor()
        self.vol = vol
        self.rng = random.Random(semilla)
        self.cambios = defaultdict(lambda: defaultdict(int))  # tabla → operación → cantidad
        self.ejemplos_pagos = []  # pagos con 2+ cambios en el día (para verificar CDC)

    # ------------------------------------------------------------- utilidades
    def _ejecutar(self, sql: str, *params, tabla: str, op: str, commit: bool = True) -> None:
        self.cur.execute(sql, *params)
        self.cambios[tabla][op] += max(self.cur.rowcount, 0)
        if commit:
            self.conn.commit()

    def _insertar_id(self, sql: str, *params, tabla: str, commit: bool = True) -> int:
        """INSERT + SCOPE_IDENTITY (no se usa OUTPUT porque las tablas tienen triggers)."""
        # NOCOUNT se vuelve a apagar al final: si quedara activo, rowcount devolvería -1
        # en los UPDATE/DELETE siguientes y el resumen de cambios saldría en cero.
        self.cur.execute("SET NOCOUNT ON; " + sql + "; SELECT CAST(SCOPE_IDENTITY() AS INT); SET NOCOUNT OFF;", *params)
        nuevo_id = self.cur.fetchone()[0]
        if commit:
            self.conn.commit()
        self.cambios[tabla]["insert"] += 1
        return nuevo_id

    def _muestra(self, sql: str, n: int) -> list:
        self.cur.execute(sql.replace("{n}", str(int(n))))
        return self.cur.fetchall()

    # --------------------------------------------------------------- eventos
    def clientes_nuevos(self) -> None:
        """Altas del día; ~5 % es un duplicado de un cliente existente (caso borde)."""
        for _ in range(self.vol.clientes_nuevos):
            if self.rng.random() < 0.05:
                fila = self._muestra("SELECT TOP 1 FirstName, LastName, Email, Phone, City, CountryCode FROM dbo.Customers ORDER BY NEWID()", 1)[0]
                datos = (fila[0], fila[1], fila[2].strip().upper(), fila[3], fila[4], fila[5])
            else:
                pais = self.rng.choices(list(PAISES), [v[0] for v in PAISES.values()])[0]
                nombre, apellido = self.rng.choice(NOMBRES_F + NOMBRES_M), f"{self.rng.choice(APELLIDOS)} {self.rng.choice(APELLIDOS)}"
                email = f"{sin_tildes(nombre).lower()}.{sin_tildes(apellido.split()[0]).lower()}{self.rng.randint(1000, 9999)}@{self.rng.choice(DOMINIOS_EMAIL)}"
                tel = PAISES[pais][2] + "".join(str(self.rng.randint(0, 9)) for _ in range(8))
                datos = (nombre, apellido, email, tel, elegir(PAISES[pais][3], self.rng), pais)
            self._insertar_id("""INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, CountryCode, Segment, SignupDate)
                                 VALUES (?, ?, ?, ?, ?, ?, 'nuevo', CAST(SYSUTCDATETIME() AS DATE))""", *datos, tabla="Customers")

    def pedidos_nuevos(self) -> None:
        """Pedidos del día con sus líneas y un pago pendiente."""
        clientes = [r[0] for r in self._muestra("SELECT TOP ({n}) CustomerID FROM dbo.Customers WHERE IsActive = 1 ORDER BY NEWID()", self.vol.pedidos_nuevos)]
        self.cur.execute("SELECT ProductID, Price FROM dbo.Products WHERE Status = 'activo'")
        productos = self.cur.fetchall()
        for cid in clientes:
            canal = elegir(CANALES, self.rng)
            elegidos = self.rng.sample(productos, self.rng.choices([1, 2, 3, 4], [50, 30, 14, 6])[0])
            lineas = [(pid, self.rng.choices([1, 2, 3], [75, 20, 5])[0], Decimal(precio)) for pid, precio in elegidos]
            total = sum((p * q for _, q, p in lineas), Decimal("0.00"))
            oid = self._insertar_id("""INSERT INTO dbo.Orders (CustomerID, OrderDate, Channel, Status, TotalAmount)
                                       VALUES (?, SYSUTCDATETIME(), ?, 'pendiente', ?)""", cid, canal, total,
                                    tabla="Orders", commit=False)  # pedido + líneas + pago = 1 transacción
            for pid, q, p in lineas:
                self._ejecutar("""INSERT INTO dbo.OrderItems (OrderID, ProductID, Quantity, UnitPrice) VALUES (?, ?, ?, ?)""",
                               oid, pid, q, p, tabla="OrderItems", op="insert", commit=False)
            metodo = elegir(METODOS_PAGO, self.rng)
            self._ejecutar("""INSERT INTO dbo.Payments (OrderID, PaymentMethod, Amount, Status, PaymentDate, TransactionRef)
                              VALUES (?, ?, ?, 'pendiente', SYSUTCDATETIME(), ?)""",
                           oid, metodo, total, None if metodo == "contraentrega" else f"TX{self.rng.randint(10**9, 10**10 - 1)}",
                           tabla="Payments", op="insert")

    def procesar_pagos(self) -> None:
        """
        Pagos pendientes que cambian de estado. Incluye los casos que SOLO CDC captura:
        - pendiente → aprobado → reembolsado en el mismo día (misma fila, 2 cambios)
        - pendiente → rechazado, y un nuevo intento pendiente → aprobado
        """
        pendientes = self._muestra("""SELECT TOP ({n}) PaymentID, OrderID, Amount FROM dbo.Payments
                                      WHERE Status = 'pendiente' AND PaymentMethod <> 'contraentrega' ORDER BY NEWID()""",
                                   self.vol.pagos_a_procesar)
        set_pago = "UPDATE dbo.Payments SET Status = ?, UpdatedAt = SYSUTCDATETIME() WHERE PaymentID = ?"
        set_pedido = "UPDATE dbo.Orders SET Status = ?, UpdatedAt = SYSUTCDATETIME() WHERE OrderID = ? AND Status IN ('pendiente','pagado')"
        for pago_id, pedido_id, monto in pendientes:
            r = self.rng.random()
            if r < 0.75:                                   # aprobado
                self._ejecutar(set_pago, "aprobado", pago_id, tabla="Payments", op="update")
                self._ejecutar(set_pedido, "pagado", pedido_id, tabla="Orders", op="update")
                if self.rng.random() < 0.04:               # ...y reembolsado el mismo día
                    time.sleep(0.2)
                    self._ejecutar(set_pago, "reembolsado", pago_id, tabla="Payments", op="update")
                    self._ejecutar(set_pedido, "cancelado", pedido_id, tabla="Orders", op="update")
                    self.ejemplos_pagos.append({"PaymentID": pago_id, "recorrido": "pendiente→aprobado→reembolsado"})
            elif r < 0.92:                                 # rechazado
                self._ejecutar(set_pago, "rechazado", pago_id, tabla="Payments", op="update")
                if self.rng.random() < 0.6:                # reintento con otro medio
                    nuevo = self._insertar_id("""INSERT INTO dbo.Payments (OrderID, PaymentMethod, Amount, Status, PaymentDate, TransactionRef)
                                                 VALUES (?, 'yape', ?, 'pendiente', SYSUTCDATETIME(), ?)""",
                                              pedido_id, monto, f"TX{self.rng.randint(10**9, 10**10 - 1)}", tabla="Payments")
                    self._ejecutar(set_pago, "aprobado", nuevo, tabla="Payments", op="update")
                    self._ejecutar(set_pedido, "pagado", pedido_id, tabla="Orders", op="update")
                    self.ejemplos_pagos.append({"PaymentID": pago_id, "recorrido": f"rechazado; reintento {nuevo}: pendiente→aprobado"})
                else:
                    self._ejecutar(set_pedido, "cancelado", pedido_id, tabla="Orders", op="update")
            # else: sigue pendiente (aún no responde la pasarela)

    def avanzar_pedidos(self) -> None:
        """pagado → enviado → entregado; ~2 % de entregados recientes se devuelven (reembolso)."""
        for desde, hacia, n in [("enviado", "entregado", self.vol.pedidos_a_entregar),
                                ("pagado", "enviado", self.vol.pedidos_a_enviar)]:
            ids = [r[0] for r in self._muestra(f"SELECT TOP ({{n}}) OrderID FROM dbo.Orders WHERE Status = '{desde}' ORDER BY NEWID()", n)]
            for oid in ids:
                self._ejecutar("UPDATE dbo.Orders SET Status = ?, UpdatedAt = SYSUTCDATETIME() WHERE OrderID = ?",
                               hacia, oid, tabla="Orders", op="update")
        # contraentrega: el pedido sale sin pago previo...
        for (oid,) in self._muestra("""SELECT TOP ({n}) o.OrderID FROM dbo.Orders o
                                       JOIN dbo.Payments p ON p.OrderID = o.OrderID
                                       WHERE o.Status = 'pendiente' AND p.PaymentMethod = 'contraentrega'
                                         AND p.Status = 'pendiente' ORDER BY NEWID()""", 15):
            self._ejecutar("UPDATE dbo.Orders SET Status = 'enviado', UpdatedAt = SYSUTCDATETIME() WHERE OrderID = ?",
                           oid, tabla="Orders", op="update")
        # ...y el pago se aprueba al entregar
        self._ejecutar("""UPDATE p SET p.Status = 'aprobado', p.UpdatedAt = SYSUTCDATETIME()
                          FROM dbo.Payments p JOIN dbo.Orders o ON o.OrderID = p.OrderID
                          WHERE p.PaymentMethod = 'contraentrega' AND p.Status = 'pendiente' AND o.Status = 'entregado'""",
                       tabla="Payments", op="update")
        devoluciones = self._muestra("""SELECT TOP ({n}) OrderID FROM dbo.Orders WHERE Status = 'entregado'
                                        AND OrderDate >= DATEADD(DAY, -30, SYSUTCDATETIME()) ORDER BY NEWID()""", 3)
        for (oid,) in devoluciones:
            self._ejecutar("UPDATE dbo.Orders SET Status = 'devuelto', UpdatedAt = SYSUTCDATETIME() WHERE OrderID = ?",
                           oid, tabla="Orders", op="update", commit=False)
            self._ejecutar("UPDATE dbo.Payments SET Status = 'reembolsado', UpdatedAt = SYSUTCDATETIME() WHERE OrderID = ? AND Status = 'aprobado'",
                           oid, tabla="Payments", op="update")

    def cambios_clientes(self) -> None:
        """Cambios de segmento (para SCD2) y correcciones de datos de contacto."""
        for cid, seg in self._muestra("SELECT TOP ({n}) CustomerID, Segment FROM dbo.Customers ORDER BY NEWID()", self.vol.cambios_segmento):
            i = self.SEGMENTOS.index(seg)
            nuevo = self.SEGMENTOS[max(0, i - 1)] if (self.rng.random() < 0.2 or i == 3) else self.SEGMENTOS[i + 1]
            self._ejecutar("UPDATE dbo.Customers SET Segment = ?, UpdatedAt = SYSUTCDATETIME() WHERE CustomerID = ?",
                           nuevo, cid, tabla="Customers", op="update")
        for cid, pais in self._muestra("SELECT TOP ({n}) CustomerID, CountryCode FROM dbo.Customers WHERE Phone IS NULL OR City IS NULL ORDER BY NEWID()",
                                        self.vol.correcciones_cliente):
            tel = PAISES[pais][2] + "".join(str(self.rng.randint(0, 9)) for _ in range(8))
            self._ejecutar("UPDATE dbo.Customers SET Phone = COALESCE(Phone, ?), City = COALESCE(City, ?), UpdatedAt = SYSUTCDATETIME() WHERE CustomerID = ?",
                           tel, elegir(PAISES[pais][3], self.rng), cid, tabla="Customers", op="update")

    def cambios_productos(self) -> None:
        """Cambios de precio (±5–15 %) y de estado del catálogo."""
        for pid, precio in self._muestra("SELECT TOP ({n}) ProductID, Price FROM dbo.Products WHERE Status = 'activo' ORDER BY NEWID()", self.vol.cambios_precio):
            nuevo = dinero(float(precio) * self.rng.choice([0.85, 0.9, 0.95, 1.05, 1.1, 1.15]))
            self._ejecutar("UPDATE dbo.Products SET Price = ?, UpdatedAt = SYSUTCDATETIME() WHERE ProductID = ?",
                           nuevo, pid, tabla="Products", op="update")
        for desde, hacia, n in [("activo", "agotado", 3), ("agotado", "activo", 3), ("activo", "descontinuado", 2)]:
            for (pid,) in self._muestra(f"SELECT TOP ({{n}}) ProductID FROM dbo.Products WHERE Status = '{desde}' ORDER BY NEWID()", n):
                self._ejecutar("UPDATE dbo.Products SET Status = ?, UpdatedAt = SYSUTCDATETIME() WHERE ProductID = ?",
                               hacia, pid, tabla="Products", op="update")

    def tickets(self) -> None:
        """Tickets nuevos y avance de estado (abierto → en_proceso → resuelto → cerrado)."""
        recientes = self._muestra("""SELECT TOP ({n}) o.OrderID, o.CustomerID FROM dbo.Orders o
                                     WHERE o.OrderDate >= DATEADD(DAY, -20, SYSUTCDATETIME()) ORDER BY NEWID()""", self.vol.tickets_nuevos)
        for oid, cid in recientes:
            asunto, plantilla, prioridad = self.rng.choice(TICKETS_TIPOS)
            self._insertar_id("""INSERT INTO dbo.SupportTickets (CustomerID, OrderID, Subject, Body, Priority, Status, OpenedAt)
                                 VALUES (?, ?, ?, ?, ?, 'abierto', SYSUTCDATETIME())""",
                              cid, oid, asunto, plantilla.format(pedido=oid, dias=self.rng.randint(2, 9), producto="producto"),
                              prioridad, tabla="SupportTickets")
        for desde, hacia in [("resuelto", "cerrado"), ("en_proceso", "resuelto"), ("abierto", "en_proceso")]:
            for (tid,) in self._muestra(f"SELECT TOP ({{n}}) TicketID FROM dbo.SupportTickets WHERE Status = '{desde}' ORDER BY NEWID()",
                                        self.vol.tickets_a_avanzar // 3):
                cierre = ", ClosedAt = SYSUTCDATETIME()" if hacia == "resuelto" else ""
                self._ejecutar(f"UPDATE dbo.SupportTickets SET Status = ?, UpdatedAt = SYSUTCDATETIME(){cierre} WHERE TicketID = ?",
                               hacia, tid, tabla="SupportTickets", op="update")

    def eliminaciones(self) -> None:
        """
        DELETE físicos (los que un watermark por fecha nunca vería):
        - el cliente quita un producto de un pedido aún pendiente (se recalcula el total)
        - un ticket enviado dos veces por error se crea y luego se elimina
        """
        candidatos = self._muestra("""SELECT TOP ({n}) i.OrderItemID, i.OrderID FROM dbo.OrderItems i
                                      JOIN dbo.Orders o ON o.OrderID = i.OrderID
                                      WHERE o.Status = 'pendiente'
                                        AND (SELECT COUNT(*) FROM dbo.OrderItems x WHERE x.OrderID = i.OrderID) > 1
                                      ORDER BY NEWID()""", self.vol.lineas_a_eliminar)
        vistos = set()
        for item_id, oid in candidatos:
            if oid in vistos:
                continue
            vistos.add(oid)
            self._ejecutar("DELETE FROM dbo.OrderItems WHERE OrderItemID = ?", item_id, tabla="OrderItems", op="delete", commit=False)
            nuevo_total = """(SELECT SUM(Quantity * UnitPrice - DiscountAmount) FROM dbo.OrderItems WHERE OrderID = ?)"""
            self._ejecutar(f"UPDATE dbo.Orders SET TotalAmount = {nuevo_total}, UpdatedAt = SYSUTCDATETIME() WHERE OrderID = ?",
                           oid, oid, tabla="Orders", op="update", commit=False)
            self._ejecutar(f"UPDATE dbo.Payments SET Amount = {nuevo_total}, UpdatedAt = SYSUTCDATETIME() WHERE OrderID = ? AND Status = 'pendiente'",
                           oid, oid, tabla="Payments", op="update")
        for cid, subj, body, prio in self._muestra("SELECT TOP ({n}) CustomerID, Subject, Body, Priority FROM dbo.SupportTickets WHERE Status = 'abierto' ORDER BY NEWID()", 2):
            dup = self._insertar_id("""INSERT INTO dbo.SupportTickets (CustomerID, Subject, Body, Priority, Status, OpenedAt)
                                       VALUES (?, ?, ?, ?, 'abierto', SYSUTCDATETIME())""", cid, subj, body, prio, tabla="SupportTickets")
            self._ejecutar("DELETE FROM dbo.SupportTickets WHERE TicketID = ?", dup, tabla="SupportTickets", op="delete")

    def ejecutar(self) -> dict:
        pasos = [("Clientes nuevos", self.clientes_nuevos), ("Pedidos nuevos", self.pedidos_nuevos),
                 ("Pagos", self.procesar_pagos), ("Avance de pedidos", self.avanzar_pedidos),
                 ("Cambios de clientes", self.cambios_clientes), ("Cambios de productos", self.cambios_productos),
                 ("Tickets", self.tickets), ("Eliminaciones", self.eliminaciones)]
        for nombre, paso in pasos:
            log(f"  {nombre}...")
            paso()
        return {"cambios": {t: dict(ops) for t, ops in self.cambios.items()},
                "pagos_con_recorrido": self.ejemplos_pagos[:20]}


def modo_dia(args) -> dict:
    conn = conectar()
    if not cdc_habilitado(conn.cursor()):
        log("AVISO: CDC no está habilitado; estos cambios no quedarán en las tablas de cambios.")
    inicio = datetime.now(timezone.utc)
    resumen = SimuladorDia(conn, VolumenDia(), args.semilla).ejecutar()
    resumen["ventana_utc"] = {"inicio": inicio.isoformat(timespec="seconds"),
                              "fin": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    conn.close()
    return resumen


# =============================================================================
# 8. PUNTO DE ENTRADA
# =============================================================================
def main() -> None:
    ap = argparse.ArgumentParser(description="Generador y simulador de datos de Andina Market (Azure SQL).")
    ap.add_argument("--modo", choices=["inicial", "dia"], required=True)
    ap.add_argument("--semilla", type=int, default=None,
                    help="Semilla aleatoria. Por defecto 2026 en 'inicial' y aleatoria en 'dia'.")
    ap.add_argument("--hasta", help="Fecha fin de la historia (AAAA-MM-DD). Por defecto hoy.")
    ap.add_argument("--clientes", type=int, default=Volumen.clientes)
    ap.add_argument("--pedidos", type=int, default=Volumen.pedidos)
    ap.add_argument("--reset", action="store_true", help="Vacía las tablas antes de cargar (solo sin CDC).")
    ap.add_argument("--dry-run", action="store_true", help="Genera en memoria y muestra estadísticas, sin escribir.")
    args = ap.parse_args()
    if args.semilla is None:
        args.semilla = 2026 if args.modo == "inicial" else int(time.time())

    t0 = time.time()
    resumen = modo_inicial(args) if args.modo == "inicial" else modo_dia(args)
    resumen.update({"modo": args.modo, "semilla": args.semilla, "segundos": round(time.time() - t0, 1)})

    print(json.dumps(resumen, indent=2, ensure_ascii=False, default=str))
    Path("logs").mkdir(exist_ok=True)
    archivo = Path("logs") / f"{args.modo}_{datetime.now():%Y%m%d_%H%M%S}{'_dryrun' if args.dry_run else ''}.json"
    archivo.write_text(json.dumps(resumen, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    log(f"Resumen guardado en {archivo}")


if __name__ == "__main__":
    main()
