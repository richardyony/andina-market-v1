"""
===============================================================================
 Archivo      : reglas_calidad.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Silver · reglas de calidad (nivel 2)
 Propósito    : Reglas de calidad como código, agrupadas por tabla y por acción.
                Las usa el pipeline declarativo (silver.py) como expectations y
                para armar la cuarentena. No depende de Spark: es un catálogo
                de reglas que se puede leer, revisar y versionar.

 Acciones
   FALLAR : el dato rompe el modelo (clave nula) → se detiene el pipeline.
   DESCARTAR : el dato es inválido y no se puede corregir → no entra a silver y
               queda en silver.cuarentena con el nombre de la regla.
   ADVERTIR : el dato es mejorable pero útil → entra a silver y la métrica queda
              en el pipeline (además se marca con una columna es_* / tiene_*).

 Regla general: las filas _op = 'D' (borrados CDC) nunca se descartan, para no
 perder un DELETE de la fuente por una regla de contenido.
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-12
===============================================================================
"""

# --- Dominios de valores permitidos (los mismos CHECK de fuentes/01_ddl_tablas.sql)
SEGMENTOS = ("nuevo", "regular", "frecuente", "vip")
PAISES = ("PE", "CO", "EC", "BO", "CL")
CANALES = ("web", "app", "tienda")
ESTADOS_PEDIDO = ("pendiente", "pagado", "enviado", "entregado", "cancelado", "devuelto")
METODOS_PAGO = ("tarjeta_credito", "tarjeta_debito", "yape", "plin", "transferencia", "contraentrega")
ESTADOS_PAGO = ("pendiente", "aprobado", "rechazado", "reembolsado")
ESTADOS_PRODUCTO = ("activo", "agotado", "descontinuado")
PRIORIDADES_TICKET = ("baja", "media", "alta")
ESTADOS_TICKET = ("abierto", "en_proceso", "resuelto", "cerrado")

# Ciudades mal escritas que llegan de la fuente → forma canónica.
# Clave en MAYÚSCULAS y sin espacios alrededor (así se compara en silver.py).
CIUDADES_CANONICAS = {
    "LIMA": "Lima", "LIMA METROPOLITANA": "Lima", "LIMA - PERÚ": "Lima",
    "BOGOTA": "Bogotá", "BOGOTÁ": "Bogotá", "BOGOTA D.C.": "Bogotá",
    "STGO": "Santiago", "SANTIAGO": "Santiago", "SANTIAGO DE CHILE": "Santiago",
    "QUITO": "Quito",
    "LA PAZ": "La Paz",
}

# Email con forma válida: algo@dominio.tld (sobre el email ya normalizado).
REGEX_EMAIL = r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"


def _en(valores) -> str:
    """Convierte una tupla de valores en la lista SQL: 'a', 'b', 'c'."""
    return ", ".join(f"'{v}'" for v in valores)


# --- Reglas por tabla ----------------------------------------------------------
# Cada regla es: nombre → expresión SQL que debe ser VERDADERA para un dato válido.
REGLAS = {
    "clientes": {
        "fallar": {"id_cliente_no_nulo": "id_cliente IS NOT NULL"},
        "descartar": {
            "segmento_valido": f"segmento IN ({_en(SEGMENTOS)})",
            "pais_valido": f"codigo_pais IN ({_en(PAISES)})",
        },
        "advertir": {
            "email_valido": f"email_normalizado RLIKE '{REGEX_EMAIL}'",
            "telefono_informado": "telefono IS NOT NULL",
            "ciudad_informada": "ciudad IS NOT NULL",
        },
    },
    "productos": {
        "fallar": {"id_producto_no_nulo": "id_producto IS NOT NULL"},
        "descartar": {
            "sku_informado": "sku IS NOT NULL",
            "precio_no_negativo": "precio >= 0",
            "estado_valido": f"estado IN ({_en(ESTADOS_PRODUCTO)})",
        },
        "advertir": {"descripcion_informada": "descripcion IS NOT NULL"},
    },
    "pedidos": {
        "fallar": {"id_pedido_no_nulo": "id_pedido IS NOT NULL"},
        "descartar": {
            "monto_total_no_negativo": "monto_total >= 0",
            "canal_valido": f"canal IN ({_en(CANALES)})",
            "estado_valido": f"estado IN ({_en(ESTADOS_PEDIDO)})",
        },
        "advertir": {"cliente_informado": "id_cliente IS NOT NULL"},
    },
    "lineas_pedido": {
        "fallar": {"id_linea_no_nula": "id_linea IS NOT NULL"},
        "descartar": {
            "cantidad_positiva": "cantidad > 0",
            "precio_no_negativo": "precio_unitario >= 0",
            "descuento_no_negativo": "descuento >= 0",
        },
        "advertir": {"descuento_menor_al_importe": "descuento <= cantidad * precio_unitario"},
    },
    "pagos": {
        "fallar": {"id_pago_no_nulo": "id_pago IS NOT NULL"},
        "descartar": {
            "monto_no_negativo": "monto >= 0",
            "metodo_valido": f"metodo_pago IN ({_en(METODOS_PAGO)})",
            "estado_valido": f"estado IN ({_en(ESTADOS_PAGO)})",
        },
        "advertir": {"referencia_informada_si_aprobado": "estado <> 'aprobado' OR referencia_transaccion IS NOT NULL"},
    },
    "tickets": {
        "fallar": {"id_ticket_no_nulo": "id_ticket IS NOT NULL"},
        "descartar": {
            "prioridad_valida": f"prioridad IN ({_en(PRIORIDADES_TICKET)})",
            "estado_valido": f"estado IN ({_en(ESTADOS_TICKET)})",
            "cierre_posterior_a_apertura": "fecha_cierre IS NULL OR fecha_cierre >= fecha_apertura",
        },
        "advertir": {"cuerpo_informado": "cuerpo IS NOT NULL"},
    },
}


def reglas(tabla: str, accion: str, proteger_borrados: bool = True) -> dict:
    """Reglas de una tabla para una acción. Con proteger_borrados, un DELETE CDC
    (_op = 'D') siempre las cumple: un borrado de la fuente nunca se pierde."""
    base = REGLAS[tabla][accion]
    if not proteger_borrados:
        return dict(base)
    return {nombre: f"(_op = 'D') OR ({expr})" for nombre, expr in base.items()}
