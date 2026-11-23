"""Catalogos y reglas fijas de Plata: zonas horarias por pais, familias de plataforma y etiqueta de skip.

Cada regla existe una sola vez aqui; Spark arma sus expresiones con estos valores y las pruebas comparan
contra las funciones de Python de este modulo. Conteos que las respaldan: DOCS/DATOS_FUENTE.md 1.3.
Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

from __future__ import annotations

import re

# Spotify usa ZZ cuando no sabe el pais: se guarda como null (mejor vacio que falso).
UNKNOWN_COUNTRIES = ("ZZ",)

# Zona IANA por pais. En paises con varias zonas se usa la de su capital o la mas poblada: es una aproximacion
# documentada; un pais fuera del catalogo deja la hora local en null y se cuenta.
COUNTRY_TIMEZONES: dict[str, str] = {
    "MX": "America/Mexico_City",
    "US": "America/New_York",
    "CA": "America/Toronto",
    "GT": "America/Guatemala",
    "SV": "America/El_Salvador",
    "HN": "America/Tegucigalpa",
    "NI": "America/Managua",
    "CR": "America/Costa_Rica",
    "PA": "America/Panama",
    "CU": "America/Havana",
    "DO": "America/Santo_Domingo",
    "PR": "America/Puerto_Rico",
    "CO": "America/Bogota",
    "VE": "America/Caracas",
    "EC": "America/Guayaquil",
    "PE": "America/Lima",
    "BO": "America/La_Paz",
    "CL": "America/Santiago",
    "AR": "America/Argentina/Buenos_Aires",
    "UY": "America/Montevideo",
    "PY": "America/Asuncion",
    "BR": "America/Sao_Paulo",
    "ES": "Europe/Madrid",
    "PT": "Europe/Lisbon",
    "GB": "Europe/London",
    "IE": "Europe/Dublin",
    "FR": "Europe/Paris",
    "DE": "Europe/Berlin",
    "IT": "Europe/Rome",
    "NL": "Europe/Amsterdam",
    "BE": "Europe/Brussels",
    "CH": "Europe/Zurich",
    "AT": "Europe/Vienna",
    "JP": "Asia/Tokyo",
    "KR": "Asia/Seoul",
    "AU": "Australia/Sydney",
}

# Familia de plataforma: el primer patron que coincide (en minusculas) gana; el orden importa
# ("web_player windows" es web, no windows; "Partner android_tv" es smart_device, no android).
# Plataforma sin patron: other; plataforma null: null.
PLATFORM_FAMILIES: tuple[tuple[str, str], ...] = (
    ("web", r"web_player|^web"),
    ("smart_device", r"partner|sonos|cast|tv|speaker|alexa|echo|playstation|xbox"),
    ("android", r"android"),
    ("ios", r"\bios\b|iphone|ipad"),
    ("windows", r"windows"),
    ("macos", r"osx|os x|macos|mac os"),
    ("linux", r"linux"),
)
OTHER_PLATFORM = "other"

DEVICE_TYPES: dict[str, str] = {
    "web": "web",
    "android": "mobile",
    "ios": "mobile",
    "windows": "desktop",
    "macos": "desktop",
    "linux": "desktop",
    "smart_device": "smart_device",
    OTHER_PLATFORM: "other",
}

# Etiqueta de skip (DATOS_FUENTE 1.3, confirmada con conteos en F3):
SKIP_REASONS = ("fwdbtn", "backbtn")  # el usuario salto: skip aunque haya escuchado mas de 30 s
COMPLETE_REASONS = ("trackdone",)  # termino sola: nunca es skip (aunque la cancion dure menos de 30 s)
SHORT_RULE_REASONS = ("endplay", "playbtn")  # el usuario cambio o detuvo: skip si duro menos de 30 s
SKIP_THRESHOLD_MS = 30_000
# Cualquier otro motivo (logout, unknown, unexpected-exit*, remote, trackerror, cosmos, vacio) no es decision
# clara del usuario: la etiqueta queda null.

SESSION_GAP_MINUTES = 30


def platform_family(platform: str | None) -> str | None:
    """Familia de una plataforma (misma regla que la expresion de Spark)."""
    if platform is None or not platform.strip():
        return None
    text = platform.lower()
    return next((fam for fam, rx in PLATFORM_FAMILIES if re.search(rx, text)), OTHER_PLATFORM)


def skip_label(reason_end: str | None, ms_played: int) -> bool | None:
    """Etiqueta de skip de una reproduccion (misma regla que la expresion de Spark)."""
    if reason_end in SKIP_REASONS:
        return True
    if reason_end in COMPLETE_REASONS:
        return False
    if reason_end in SHORT_RULE_REASONS:
        return ms_played < SKIP_THRESHOLD_MS
    return None


def offline_epoch_seconds(value: int | None) -> float | None:
    """offline_timestamp a segundos (H-003): 10 digitos = segundos, 13 = milisegundos, otro = null."""
    if value is None:
        return None
    if 10**9 <= value < 10**10:
        return float(value)
    if 10**12 <= value < 10**13:
        return value / 1000
    return None
