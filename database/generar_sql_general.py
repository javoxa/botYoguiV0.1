#!/usr/bin/env python3
"""
Genera inserts SQL para información general, contactos, calendario, etc.
Versión corregida: maneja arrays vacíos con tipo explícito y filtra líneas inválidas.
"""

import json
import re
from pathlib import Path

# ========== CONFIG ==========
PROJECT_ROOT = Path(__file__).parent.parent
KNOWLEDGE_BASE = PROJECT_ROOT / "frontend" / "knowledge_base"
SOURCES_DIR = KNOWLEDGE_BASE / "sources"
OUTPUT_SQL = PROJECT_ROOT / "database" / "informacion_general.sql"

# ========== UTILIDADES ==========
def limpiar(texto):
    """Limpia el texto para SQL (escapa comillas simples) y recorta espacios."""
    if not texto:
        return ""
    return texto.strip().replace("'", "''")

def detectar_categoria(texto):
    texto_low = texto.lower()
    if any(k in texto_low for k in ["contacto", "email", "teléfono", "whatsapp", "web", "http"]):
        return "Contacto"
    if any(k in texto_low for k in ["calendario", "inicio de clases", "receso", "exámenes", "mesa"]):
        return "Calendario"
    if any(k in texto_low for k in ["inscripción", "preinscripción", "matrícula"]):
        return "Inscripción"
    if any(k in texto_low for k in ["sede", "ubicación", "dirección", "central", "orán", "tartagal"]):
        return "Ubicación"
    return "General"

def detectar_facultad(texto):
    texto_low = texto.lower()
    facultades_map = {
        "exactas": "Exactas",
        "ingeniería": "Ingeniería",
        "humanidades": "Humanidades",
        "salud": "Salud",
        "naturales": "Naturales",
        "económicas": "Económicas",
        "orán": "Orán",
        "tartagal": "Tartagal"
    }
    for key, fac in facultades_map.items():
        if key in texto_low:
            return fac
    return "General"

def extraer_keywords(texto, max_keywords=8):
    """Extrae palabras clave relevantes del texto."""
    palabras = re.findall(r'\b[a-záéíóúñ]{4,}\b', texto.lower())
    stopwords = {
        "para", "con", "por", "entre", "sobre", "esta", "este", "como",
        "mas", "pero", "cuando", "donde", "que", "del", "los", "las",
        "una", "unos", "unas", "desde", "hacia", "tiene", "ser", "son",
        "fue", "para", "todo", "toda", "más", "muy", "puede", "info"
    }
    keywords = list(set(p for p in palabras if p not in stopwords))
    return keywords[:max_keywords]

def generar_insert(contenido, categoria, facultad, keywords):
    """Genera una línea SQL con manejo correcto de array vacío."""
    if not contenido or len(contenido) < 5:
        return None  # Descartar contenido muy corto
    contenido_limpio = limpiar(contenido)
    if not contenido_limpio:
        return None
    if keywords:
        kw_sql = ", ".join(f"'{k}'" for k in keywords)
        array_sql = f"ARRAY[{kw_sql}]"
    else:
        array_sql = "ARRAY[]::text[]"
    return f"INSERT INTO fragmentos_conocimiento (contenido, categoria, facultad, palabras_clave) VALUES ('{contenido_limpio}', '{categoria}', '{facultad}', {array_sql});"

# ========== PROCESAR CADA FUENTE ==========
inserts = []

# ---- 1. Contactos desde JSONL (si existe) ----
contactos_file = SOURCES_DIR / "contactos.jsonl"
if contactos_file.exists():
    with open(contactos_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                print(f"⚠️ Línea {line_num} de contactos.jsonl inválida, se omite.")
                continue
            # Adaptar según la estructura real de tu JSONL
            # Ejemplo: suponemos campos "nombre", "tipo", "contacto"
            nombre = data.get("nombre", "")
            tipo = data.get("tipo", "Contacto")
            contacto = data.get("contacto", "")
            contenido = f"{nombre}: {contacto}".strip()
            if not contenido:
                continue
            categoria = detectar_categoria(contenido)
            facultad = detectar_facultad(contenido)
            keywords = extraer_keywords(contenido)
            sql = generar_insert(contenido, categoria, facultad, keywords)
            if sql:
                inserts.append(sql)

# ---- 2. Información general desde .txt ----
general_txt = KNOWLEDGE_BASE / "informacion_general.txt"
if general_txt.exists():
    with open(general_txt, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Descartar líneas muy cortas o con solo signos
            if len(line) < 5:
                continue
            contenido = line
            categoria = detectar_categoria(line)
            facultad = detectar_facultad(line)
            keywords = extraer_keywords(line)
            sql = generar_insert(contenido, categoria, facultad, keywords)
            if sql:
                inserts.append(sql)

# ---- 3. Calendario desde becas_y_calendario.txt ----
becas_cal = KNOWLEDGE_BASE / "becas_y_calendario.txt"
if becas_cal.exists():
    with open(becas_cal, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("-"):
                continue
            # Solo líneas que parezcan de calendario (para no repetir becas)
            if "calendario" in line.lower() or "clases" in line.lower() or "exámenes" in line.lower() or "mesa" in line.lower():
                contenido = line
                categoria = "Calendario"
                facultad = detectar_facultad(line)
                keywords = extraer_keywords(line)
                sql = generar_insert(contenido, categoria, facultad, keywords)
                if sql:
                    inserts.append(sql)

# ---- 4. Fragmentos varios desde all_chunks.txt ----
all_chunks = KNOWLEDGE_BASE / "all_chunks.txt"
if all_chunks.exists():
    with open(all_chunks, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or len(line) < 5:
                continue
            contenido = line
            categoria = detectar_categoria(line)
            facultad = detectar_facultad(line)
            keywords = extraer_keywords(line)
            sql = generar_insert(contenido, categoria, facultad, keywords)
            if sql:
                inserts.append(sql)

# ========== ESCRIBIR ARCHIVO SQL ==========
OUTPUT_SQL.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_SQL, "w", encoding="utf-8") as f:
    f.write("-- Información general generada automáticamente\n")
    for sql in inserts:
        f.write(sql + "\n")

print(f"✅ Generados {len(inserts)} inserts en {OUTPUT_SQL}")
