# ./frontend/bot/retriever/retriever.py
import asyncio
import time
import re
import logging
from typing import List, Tuple
import asyncpg
from .models import SearchResult, ResponseMode
from .config import logger

class PostgresRetriever:
    def __init__(self, db_url: str, debug_mode: bool = False):
        self.db_url = db_url
        self.pool = None
        self.connected = False
        self.debug_mode = debug_mode
        self.stats = {
            "queries": 0,
            "errors": 0,
            "fragments": 0
        }
        self.last_connect_attempt = 0
        self.connect_retry_delay = 2  # segundos entre reintentos

        # --- Keywords Carrera (como en la versión combinada anterior) ---
        self.carrera_keywords = {
            # Exactas
            'fisica', 'física', 'matematica', 'matemática', 'quimica', 'química',
            'informatica', 'informática', 'sistemas', 'computacion', 'computación',
            'programacion', 'programación', 'estadistica', 'estadística',
            'electronica', 'electrónica', 'energia', 'energía', 'renovable',
            'bromatologia', 'bromatología',
            # Ingenierías
            'ingenieria', 'ingeniería', 'civil', 'industrial', 'quimica',
            'electromecanica', 'electromecánica', 'alimentos',
            # Salud
            'medicina', 'enfermeria', 'enfermería', 'nutricion', 'nutrición',
            'farmacia',
            # Humanidades
            'derecho', 'abogacia', 'abogacía', 'administracion', 'administración',
            'economia', 'economía', 'contador', 'contaduria', 'contaduría',
            'comunicacion', 'comunicación', 'educacion', 'educación', 'historia',
            'filosofia', 'filosofía', 'letras', 'antropologia', 'antropología',
            # Naturales
            'biologia', 'biología', 'geologia', 'geología', 'agronomia', 'agronomía',
            'recursos', 'medioambiente', 'medio ambiente',
            # General
            'licenciatura', 'profesorado', 'tecnicatura', 'analista', 'maestria',
            'maestría', 'doctorado', 'posgrado', 'especializacion', 'especialización'
        }
        self.explicit_carrera_terms = {
            'carrera', 'carreras', 'estudiar', 'estudio', 'estudios',
            'titulo', 'título', 'grado', 'pregrado', 'posgrado',
            'duracion', 'duracción', 'años', 'año', 'cuanto dura'
        }
        # --- Fin Keywords Carrera ---

        # --- Keywords Consultas Generales (como en la versión combinada anterior) ---
        self.list_queries_keywords = {
            "hay", "existen", "disponibles", "cual", "cuales", "lista", "listado",
            "ofrece", "tienes", "cuantas", "carrera", "carreras", "beca", "becas",
            "curso", "cursos", "programa", "programas", "materia", "materias",
            "asignatura", "asignaturas", "facultad", "facultades", "area", "areas",
            "departamento", "departamentos"
        }
        # --- Fin Keywords Generales ---

    async def connect(self) -> bool:
        """Intentar conectar a PostgreSQL con reintentos"""
        current_time = time.time()
        if current_time - self.last_connect_attempt < self.connect_retry_delay:
            return self.connected
        self.last_connect_attempt = current_time
        if self.connected:
            return True
        try:
            self.pool = await asyncpg.create_pool(
                self.db_url,
                min_size=2,
                max_size=20,
                command_timeout=30
            )
            async with self.pool.acquire() as conn:
                if self.debug_mode:
                    try:
                        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
                        await conn.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
                    except Exception as e:
                        logger.warning(f"Advertencia al crear extensiones: {e}")
                self.stats["fragments"] = await conn.fetchval(
                    "SELECT COUNT(*) FROM fragmentos_conocimiento"
                )
                self.connected = True
                logger.info("✅ PostgreSQL conectado | Fragmentos: %d", self.stats["fragments"])
                return True
        except Exception as e:
            self.connected = False
            logger.error("❌ PostgreSQL error: %s", str(e))
            return False

    async def disconnect(self):
        """Cerrar conexión pool al apagar"""
        if self.pool:
            try:
                await self.pool.close()
                self.connected = False
                logger.info("✅ Pool PostgreSQL cerrado")
            except Exception as e:
                logger.error("❌ Error al cerrar pool PostgreSQL: %s", str(e))

    def _remove_accents(self, text: str) -> str:
        """Elimina acentos de un texto - Código 2"""
        accents = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
            'ñ': 'n', 'Ñ': 'N'
        }
        for acc, no_acc in accents.items():
            text = text.replace(acc, no_acc)
        return text

    def _clean_query_terms(self, query: str) -> Tuple[List[str], bool]:
        """
        Limpia la consulta y extrae términos de búsqueda.
        Retorna: (términos, es_consulta_de_carrera)
        Incorpora lógica de Código 2 para detección de carrera.
        """
        query_norm = self._remove_accents(query.lower())
        clean = re.sub(r"[^\w\s]", " ", query_norm)
        words = clean.split()

        stopwords = {
            # Preposiciones básicas
            'a', 'ante', 'bajo', 'con', 'de', 'desde', 'en', 'entre', 'hacia',
            'hasta', 'para', 'por', 'según', 'sin', 'so', 'sobre', 'tras',

            # Artículos
            'el', 'la', 'lo', 'los', 'las', 'un', 'una', 'unos', 'unas',

            # Conjunciones
            'y', 'o', 'u', 'ni', 'pero', 'mas', 'sino', 'aunque',

            # Pronombres personales
            'yo', 'tú', 'él', 'ella', 'usted', 'nosotros', 'vosotros', 'ellos', 'ellas', 'ustedes',
            'me', 'te', 'se', 'nos', 'os',

            # Verbos comunes poco específicos
            'hay', 'tener', 'tengo', 'tiene', 'tienen', 'haber', 'ser', 'es', 'son', 'era',
            'estar', 'está', 'están', 'hacer', 'hace', 'hacen', 'poder', 'puede', 'pueden',
            'deber', 'debe', 'deben', 'querer', 'quiere', 'quieren',

            # Adverbios y otras palabras genéricas
            'muy', 'mucho', 'poco', 'algo', 'nada', 'todo', 'también', 'además',
            'solo', 'solamente', 'incluso', 'inclusive', 'asimismo',

            # Preposiciones compuestas
            'al', 'del',  # Contracciones importantes

            # Demostrativos
            'este', 'esta', 'esto', 'estos', 'estas',
            'ese', 'esa', 'eso', 'esos', 'esas',
            'aquel', 'aquella', 'aquello', 'aquellos', 'aquellas',
        }


        terms = [w for w in words if len(w) >= 3 and w not in stopwords]

        is_carrera_query = False
        if any(term in self.explicit_carrera_terms for term in terms):
            is_carrera_query = True
        elif any(term in self.carrera_keywords for term in terms):
            general_terms = {'que', 'como', 'donde', 'cuando', 'informacion', 'información'}
            if not any(gterm in query_norm.split() for gterm in general_terms):
                is_carrera_query = True

        if not terms and len(clean.strip()) >= 4:
            return ([clean.strip()[:20]], is_carrera_query)

        return (terms[:3], is_carrera_query)

    def _is_general_list_query(self, query: str) -> bool:
        """
        Detecta si la consulta es del tipo general de listado (e.g., "qué carreras hay").
        Código 1 adaptado para usar también _remove_accents.
        """
        clean_query_lower = self._remove_accents(query.lower())
        query_words = set(clean_query_lower.split())
        has_list_keyword = bool(self.list_queries_keywords.intersection(query_words))
        if has_list_keyword and len(query_words) <= 5:
             specific_indicators = {'de', 'en', 'para', 'con', 'sobre', 'acerca', 'del', 'de', 'la', 'al'}
             if not specific_indicators.intersection(query_words):
                 return True
        return False

    async def retrieve(
        self, query: str, limit: int = 20
    ) -> Tuple[str, List[SearchResult], ResponseMode]:
        self.stats["queries"] += 1
        if not await self.connect():
            await asyncio.sleep(1)
            if not await self.connect():
                return "Error de base de datos.", [], ResponseMode.FALLBACK

        try:
            terms, is_carrera_query = self._clean_query_terms(query)
            is_general_query = self._is_general_list_query(query)

            async with self.pool.acquire() as conn:
                if not terms and not is_general_query:
                    rows = await conn.fetch(
                        """
                        SELECT id, contenido, categoria, facultad, palabras_clave, descripcion -- Añadido descripcion
                        FROM fragmentos_conocimiento
                        ORDER BY usado_count DESC
                        LIMIT $1
                        """,
                        limit
                    )
                elif is_general_query:
                    logger.debug(f"Consulta general detectada: '{query}', buscando carreras o becas...")
                    rows = await conn.fetch(
                        """
                        SELECT id, contenido, categoria, facultad, palabras_clave, descripcion -- Añadido descripcion
                        FROM fragmentos_conocimiento
                        WHERE LOWER(categoria) LIKE ANY(ARRAY['%carrera%', '%beca%'])
                        ORDER BY usado_count DESC, relevancia DESC
                        LIMIT $1
                        """,
                        limit
                    )
                else:
                    # --- Lógica de búsqueda principal, ahora incluyendo 'descripcion' ---
                    similarity_conditions = []
                    ilike_conditions = []
                    keyword_conditions = []
                    params = []

                    for i, term in enumerate(terms):
                        # ILIKE: Buscar en contenido Y descripcion (si existe y no es NULL)
                        # La cláusula OR maneja el caso de descripcion NULL
                        ilike_conditions.append(f"(contenido ILIKE unaccent(${len(params) + 1}) OR (descripcion IS NOT NULL AND descripcion ILIKE unaccent(${len(params) + 1})))")
                        params.append(f"%{term}%")

                        # Similarity: Calcular similitud en contenido Y descripcion, tomar el máximo
                        # COALESCE maneja el caso de que descripcion sea NULL, devolviendo 0 para similarity
                        similarity_conditions.append(f"GREATEST(similarity(unaccent(contenido), unaccent(${len(params) + 1}::text)), COALESCE(similarity(unaccent(descripcion), unaccent(${len(params) + 1}::text)), 0)) > 0.3")
                        params.append(term)

                        # Keyword: Buscar en palabras_clave
                        keyword_conditions.append(f"${len(params) + 1} = ANY(palabras_clave)")
                        params.append(term)

                    all_conditions = " OR ".join(ilike_conditions + similarity_conditions + keyword_conditions)

                    if is_carrera_query:
                         order_clause = """
                             CASE
                                 WHEN contenido ILIKE '%carrera%' THEN 1
                                 WHEN contenido ILIKE '%licenciatura%' THEN 2
                                 WHEN contenido ILIKE '%profesorado%' THEN 3
                                 WHEN contenido ILIKE '%tecnicatura%' THEN 4
                                 ELSE 5
                             END,
                             GREATEST(similarity(unaccent(contenido), unaccent(${len(params) + 1}::text)), COALESCE(similarity(unaccent(descripcion), unaccent(${len(params) + 1}::text)), 0), 0) DESC,
                             usado_count DESC
                         """
                         params.append(terms[0]) # Parámetro para similarity en ORDER BY
                         similarity_param_index = len(params)
                    else:
                        order_clause = "GREATEST(similarity(unaccent(contenido), unaccent(${len(params) + 1}::text)), COALESCE(similarity(unaccent(descripcion), unaccent(${len(params) + 1}::text)), 0), 0) DESC, usado_count DESC"
                        params.append(terms[0]) # Parámetro para similarity en ORDER BY
                        similarity_param_index = len(params)

                    params.append(limit)

                    sql = f"""
                    SELECT id, contenido, categoria, facultad, palabras_clave, descripcion -- Añadido descripcion
                    FROM fragmentos_conocimiento
                    WHERE {all_conditions}
                    ORDER BY {order_clause}
                    LIMIT ${similarity_param_index}
                    """
                    rows = await conn.fetch(sql, *params)

                if not rows:
                    return "No se encontró información.", [], ResponseMode.FALLBACK

                # Mapear resultados, ahora incluyendo la descripcion
                results = [
                    SearchResult(
                        id=r["id"],
                        content=r["contenido"],
                        category=r["categoria"],
                        faculty=r["facultad"],
                        score=1.0,
                        keywords=r["palabras_clave"] or [],
                        description=r["descripcion"] # <-- Nuevo campo mapeado
                    )
                    for r in rows
                ]

                for r in results:
                    await conn.execute(
                        "UPDATE fragmentos_conocimiento SET usado_count = usado_count + 1 WHERE id = $1",
                        r.id
                    )

                context = "\n".join(r.content for r in results)
                total_len = sum(len(r.content) for r in results)

                if is_carrera_query and total_len < 1200:
                    mode = ResponseMode.DIRECT
                elif total_len < 800:
                    mode = ResponseMode.DIRECT
                else:
                    mode = ResponseMode.LLM

                return context, results, mode
        except Exception as e:
            self.stats["errors"] += 1
            logger.error("❌ Retrieve error: %s", str(e))
            return "Error consultando la base.", [], ResponseMode.FALLBACK

    def build_direct_response(self, results: List[SearchResult]) -> str:
        if not results:
            return "No encontré información específica."
        # Incluir la descripción si está disponible para respuestas directas
        response_parts = []
        for r in results[:3]: # Limitar a los 3 primeros
            part = r.content
            if r.description:
                 part += f" [{r.description}]"
            response_parts.append(part)
        return "\n\n".join(response_parts)
