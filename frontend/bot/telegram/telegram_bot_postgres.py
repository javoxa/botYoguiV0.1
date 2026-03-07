#!/usr/bin/env python3
"""
Bot UNSA - VERSIÓN FINAL MODULAR
Prompts completamente externalizados (sin hardcode)
Con manejo de timeouts en envío de mensajes
Incluye respuestas sobre el bot (quién es, desarrollador, etc.)
"""

import asyncio
import aiohttp
import hashlib
import time
import re
import signal
import sys
from collections import defaultdict
from typing import Optional
import yaml
from pathlib import Path

from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from telegram.error import TimedOut, NetworkError  # Importar errores de red

# Importaciones desde los módulos
from ..config import (
    TOKEN, DEBUG_MODE, INFERENCE_API_URL, DATABASE_URL,
    REQUEST_TIMEOUT, RETRY_ATTEMPTS, RETRY_DELAY,
    RATE_LIMIT_WINDOW, RATE_LIMIT_MAX_REQUESTS,
    logger
)
from ..models import ResponseMode, SearchResult
from ..utils import RateLimiter, anonymize_message, escape_md
from ..retriever import PostgresRetriever


# ----------------------------------------------------------------------
# Carga de prompts desde archivo YAML (sin fallback hardcodeado)
# ----------------------------------------------------------------------
def load_prompts(file_name: str = "prompts.yaml") -> dict:
    """
    Carga las plantillas de prompts desde un archivo YAML.
    Si el archivo no existe o está malformado, lanza una excepción.
    """
    prompts_path = Path(__file__).parent / file_name
    if not prompts_path.exists():
        raise FileNotFoundError(f"Archivo de prompts no encontrado: {prompts_path}")

    try:
        with open(prompts_path, 'r', encoding='utf-8') as f:
            prompts = yaml.safe_load(f)
        if not isinstance(prompts, dict):
            raise ValueError("El archivo YAML no contiene un diccionario válido")
        if "llm" not in prompts:
            raise KeyError("El archivo YAML debe contener una clave 'llm'")
        required_keys = ["main", "greeting", "explanatory_with_prev", "explanatory_with_new"]
        for key in required_keys:
            if key not in prompts["llm"]:
                raise KeyError(f"Falta la clave 'llm.{key}' en el archivo de prompts")
        return prompts
    except Exception as e:
        logger.error(f"Error al cargar prompts: {e}")
        raise


class BotManager:
    def __init__(self, retriever: PostgresRetriever, prompts: dict):
        self.retriever = retriever
        self.prompts = prompts['llm']
        self.start_time = time.time()
        self.user_stats = {"messages": 0, "users": set()}
        self.last_message_time = {}
        self.limiter = RateLimiter(RATE_LIMIT_WINDOW, RATE_LIMIT_MAX_REQUESTS)
        self.session: Optional[aiohttp.ClientSession] = None
        self.stop_event = asyncio.Event()
        self.last_results_by_user = {}

    # Preguntas sobre el bot mismo
    ABOUT_TRIGGERS = {
        "quien eres", "quién eres", "quien sos", "quién sos",
        "quien te creo", "quién te creó", "quien te hizo", "quién te hizo",
        "tu creador", "tus creadores", "desarrollador", "desarrolladores",
        "que modelo usas", "qué modelo usas", "modelo", "llm",
        "codigo fuente", "código fuente", "github", "repositorio", "repo",
        "como funcionas", "cómo funcionas", "tecnologías", "tecnologias",
        "que eres", "qué eres", "que sos", "qué sos", "quién te programo",
        "quien te programo", "quien te desarrolló","quién te desarrolló",
        "quien te desarrollo"
    }

    ABOUT_MESSAGE = """
🤖 *YoguI A - Asistente Virtual del grupo de Investigación FiEstA (Física Estadística Aplicada) del Departamento de Física de la UNSa*

*Desarrollador:* Javier Gutierrez, JTP del Departamento de Física de la UNSa
*Repositorio:* [GitHub](https://github.com/javoxa/botYoguiV0.0)
*Modelo de IA:* Qwen2-7B-Instruct-AWQ (vLLM)
*Tecnologías:* Python, PostgreSQL, vLLM, FastAPI, asyncpg, python-telegram-bot
*Versión:* 0.0

*Funcionalidades:*
• Información sobre carreras, becas, contacto y calendario
• Respuestas generadas por IA basadas en la base de conocimiento
• Búsqueda semántica en PostgreSQL
• Por el momento solo con la base de datos del Departamento de Física de Ciencias Exactas
• Para otras facultades puede la información ser imprecisa

*Nota:* Este es un bot no oficial desarrollado con fines educativos y de apoyo a la comunidad universitaria.

*Sobre el grupo FiEstA:* Es un equipo unipersonal, que se dedica al modelado de sistamas biologicos fuera del equilibrio
y actualmente se dedica al desarrollo de arquitectura (Agentes de IA) para Inteligencia Artificial

"""

    async def _safe_reply(self, update: Update, text: str, parse_mode: str = None, max_retries: int = 3):
        """
        Envía un mensaje de forma segura con reintentos en caso de timeout o error de red.
        """
        for attempt in range(max_retries):
            try:
                if parse_mode:
                    await update.message.reply_text(text, parse_mode=parse_mode)
                else:
                    await update.message.reply_text(text)
                return  # Éxito, salimos
            except (TimedOut, NetworkError) as e:
                logger.warning(f"Error de red al enviar mensaje (intento {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    # Esperar antes de reintentar (backoff simple)
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    # Último intento falló, registramos y no podemos hacer más
                    logger.error(f"No se pudo enviar mensaje después de {max_retries} intentos: {text[:100]}...")
                    # Opcional: notificar por otro lado, pero no tenemos contexto
            except Exception as e:
                # Otros errores no relacionados con red, no reintentamos
                logger.error(f"Error inesperado al enviar mensaje: {e}")
                break

    async def init_session(self):
        """Inicializa la sesión HTTP persistente"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=5)
            self.session = aiohttp.ClientSession(timeout=timeout)
            logger.info("✅ Sesión HTTP inicializada")

    async def close_session(self):
        """Cerrar sesión HTTP limpiamente"""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
                logger.info("✅ Sesión HTTP cerrada")
            except Exception as e:
                logger.error("❌ Error al cerrar sesión HTTP: %s", str(e))

    async def close_resources(self):
        """Cierra todos los recursos limpiamente"""
        tasks = [
            self.close_session(),
            self.retriever.disconnect()
        ]

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("✅ Todos los recursos cerrados correctamente")
        except Exception as e:
            logger.error("❌ Error al cerrar recursos: %s", str(e))

    def signal_handler(self):
        """Manejador de señales para cierre limpio"""
        logger.info("🛑 Recibida señal de parada, cerrando recursos...")
        self.stop_event.set()

    def _build_prompt(self, question: str, context: str) -> str:
        return self.prompts['main'].format(context=context, question=question)

    async def _call_llm(self, prompt: str, user_hash: str) -> str:
        max_retries = RETRY_ATTEMPTS
        base_delay = RETRY_DELAY

        for attempt in range(max_retries + 1):
            try:
                if self.session is None or self.session.closed:
                    await self.init_session()

                async with self.session.post(
                    INFERENCE_API_URL,
                    json={
                        "prompt": prompt,
                        "user_id": user_hash,
                        "max_tokens": 500,
                        "temperature": 0.2
                    }
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        answer = data.get("response", "").strip()
                        if answer:
                            return answer
                        logger.warning(f"Respuesta vacía de IA en intento {attempt+1}")
                    else:
                        logger.warning(f"Error HTTP {resp.status} en intento {attempt+1}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Error de conexión en intento {attempt+1}: {e}")

            if attempt < max_retries:
                delay = base_delay * (attempt + 1)
                logger.info(f"Esperando {delay:.1f}s antes de reintento {attempt+2}/{max_retries+1}")
                await asyncio.sleep(delay)

        logger.error(f"Todos los intentos de conexión a IA fallaron para usuario {user_hash}")
        return ""

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._safe_reply(
            update,
            "👋 YoguI A, el asistente virtual te da la bienvenida\n\n"
            "¿En qué puedo ayudarte?\n"
            "• Carreras y programas de estudio\n"
            "• Información sobre becas\n"
            "• Fechas de inscripción\n"
            "• Trámites administrativos\n"
            "• Contactos y ubicaciones\n\n"
            "*Comandos disponibles:*\n"
            "/help – Ver todos los comandos\n"
            "/stats – Estadísticas del bot\n"
            "/diagnose – Estado del sistema\n"
            "/about – Información sobre el bot\n\n"
            "*Enlaces útiles:*\n"
            "🔗 https://www.unsa.edu.ar    \n"
            "🔗 https://exactas.unsa.edu.ar    ",
            parse_mode="Markdown"
        )

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._safe_reply(
            update,
            "🤖 YoguI A, el asistente virtual\n\n"
            "*Comandos disponibles:*\n"
            "/start – Mensaje de bienvenida\n"
            "/help – Esta ayuda\n"
            "/stats – Estadísticas del bot\n"
            "/diagnose – Estado del sistema\n"
            "/about – Información sobre el bot\n\n"
            "*También podés escribir tu consulta directamente.*\n"
            "Ejemplos:\n"
            "• \"¿Hay becas?\"\n"
            "• \"Carreras de fisica\"\n"
            "• \"Contacto de exactas\"\n"
            "• \"Fechas de inscripción 2026\"",
            parse_mode="Markdown"
        )

    # Para semántica
    EXPLANATORY_TRIGGERS = {
        "de que se trata", "de qué se trata", "de que se tratan",
        "diferencia", "me conviene", "salida laboral",
        "orientacion", "orientación",
        "perfil", "en que consiste",
        "qué hace", "que hace",
        "algo facil", "para qué sirve", "para que sirve",
        "de que se trabaja"
    }

    def is_explanatory_question(self, msg: str) -> bool:
        msg = msg.lower()
        return any(t in msg for t in self.EXPLANATORY_TRIGGERS)

    async def about(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Comando /about: muestra información del bot."""
        await self._safe_reply(update, self.ABOUT_MESSAGE, parse_mode="Markdown")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if self.stop_event.is_set():
            return

        msg = update.message.text.strip()
        user_id = update.effective_user.id

        if not self.limiter.is_allowed(user_id):
            await self._safe_reply(
                update,
                "⏳ Has excedido el límite de solicitudes. "
                "Por favor, espera unos minutos antes de volver a intentarlo."
            )
            return

        # Detectar preguntas sobre el bot
        msg_lower = msg.lower()
        if any(trigger in msg_lower for trigger in self.ABOUT_TRIGGERS):
            await self._safe_reply(update, self.ABOUT_MESSAGE, parse_mode="Markdown")
            return

        now = time.time()
        last = self.last_message_time.get(user_id, 0)
        if now - last < 1.5:
            return
        self.last_message_time[user_id] = now

        user_hash = hashlib.md5(str(user_id).encode()).hexdigest()[:8]
        self.user_stats["users"].add(user_hash)
        self.user_stats["messages"] += 1

        logger.info("📩 Usuario %s: %s", user_hash, anonymize_message(msg))

        await context.bot.send_chat_action(
            chat_id=update.effective_chat.id,
            action=ChatAction.TYPING
        )

        GREETINGS = {"hola", "buenas", "buen", "hey", "saludos",
                     "como va", "hi", "holaa", "holaaa"
                     }
        msg_norm = re.sub(r"[^\w\s]", "", msg.lower())
        tokens = msg_norm.split()
        is_greeting = any(t in GREETINGS for t in tokens)

        if is_greeting:
            prompt = self.prompts['greeting'].format(msg=msg)
            answer = await self._call_llm(prompt, user_hash)

            if answer:
                await self._safe_reply(update, answer)
            else:
                await self._safe_reply(
                    update,
                    "👋 YoguI A, el asistente no oficial te saluda.\n\n"
                    "Podés preguntarme sobre becas, carreras, inscripciones o trámites.\n"
                    "Usá /help para ver los comandos."
                )
            return

        if self.is_explanatory_question(msg):
            prev_results = self.last_results_by_user.get(user_hash)
            if prev_results:
                careers_list = "\n".join(f"- {r.content}" for r in prev_results)
                prompt = self.prompts['explanatory_with_prev'].format(
                    careers_list=careers_list, msg=msg
                )
                answer = await self._call_llm(prompt, user_hash)
                if answer:
                    await self._safe_reply(update, answer)
                    return

        context_text, results, mode = await self.retriever.retrieve(msg, limit=20)

        if results and any("Carrera" in r.content for r in results):
            self.last_results_by_user[user_hash] = results

        if self.is_explanatory_question(msg):
            prev_results = self.last_results_by_user.get(user_hash)
            if prev_results:
                palabras_pregunta = set(msg.lower().split())
                filtered_careers = []
                for r in prev_results:
                    if any(p in r.content.lower() for p in palabras_pregunta) or len(palabras_pregunta) < 4:
                        filtered_careers.append(r)
                if not filtered_careers:
                    filtered_careers = prev_results[:3]
                careers_list = "\n".join(f"- {r.content}" for r in filtered_careers)
                prompt = self.prompts['explanatory_with_new'].format(
                    careers_list=careers_list, msg=msg
                )
                answer = await self._call_llm(prompt, user_hash)
                if answer:
                    await self._safe_reply(update, answer)
                    return

        if mode == ResponseMode.FALLBACK:
            await self._safe_reply(
                update,
                "No tengo información específica sobre eso.\nVisitá https://www.unsa.edu.ar"
            )
            return

        if mode == ResponseMode.DIRECT:
            if self.is_explanatory_question(msg):
                careers_list = "\n".join(f"- {r.content}" for r in results)
                prompt = self.prompts['explanatory_with_new'].format(
                    careers_list=careers_list, msg=msg
                )
                answer = await self._call_llm(prompt, user_hash)
                if answer:
                    await self._safe_reply(update, answer)
                    return
            response = self.retriever.build_direct_response(results)
            await self._safe_reply(update, response)
            return

        try:
            prompt = self._build_prompt(msg, context_text)
            answer = await self._call_llm(prompt, user_hash)

            if answer:
                await self._safe_reply(update, answer)
                return

            logger.info(f"Falló IA para usuario {user_hash}, usando fallback directo")
            fallback_response = (
                "⚠️ *Servicio de IA temporalmente no disponible*\n\n"
                f"{escape_md(self.retriever.build_direct_response(results))}\n\n"
                "_Información obtenida directamente de la base de datos_"
            )
            await self._safe_reply(update, fallback_response, parse_mode="Markdown")

        except Exception as e:
            logger.error("❌ API error: %s", str(e))
            fallback_response = (
                "⚠️ *Ocurrió un error inesperado*\n\n"
                f"{escape_md(self.retriever.build_direct_response(results))}\n\n"
                "_Información obtenida directamente de la base de datos_"
            )
            await self._safe_reply(update, fallback_response, parse_mode="Markdown")

    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        r = self.retriever.stats
        uptime = time.time() - self.start_time
        hours, remainder = divmod(int(uptime), 3600)
        minutes, _ = divmod(remainder, 60)

        await self._safe_reply(
            update,
            f"📊 *Estadísticas*\n\n"
            f"*Uptime:* {hours}h {minutes}m\n"
            f"*Base de datos:*\n"
            f"• Consultas: {r['queries']}\n"
            f"• Fragmentos: {r['fragments']}\n"
            f"• Errores: {r['errors']}\n\n"
            f"*Usuarios:*\n"
            f"• Únicos: {len(self.user_stats['users'])}\n"
            f"• Mensajes: {self.user_stats['messages']}\n\n"
            f"*Rate Limit:* {RATE_LIMIT_MAX_REQUESTS} solicitudes por {RATE_LIMIT_WINDOW} segundos",
            parse_mode="Markdown"
        )

    async def diagnose(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        r = self.retriever.stats
        db_status = "🟢 Conectado" if self.retriever.connected else "🔴 Error"
        ia_status = "🟢 OK"

        try:
            if self.session is None or self.session.closed:
                await self.init_session()
            base_url = INFERENCE_API_URL.rsplit('/', 1)[0]
            health_url = f"{base_url}/health"
            async with self.session.get(health_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status_msg = data.get("status", "unknown")
                    queue_load = data.get("queue_load_percent", 0)
                    ia_status = f"🟢 {status_msg} - {queue_load}% cola"
                else:
                    ia_status = f"🔴 Error HTTP {resp.status}"
        except Exception as e:
            ia_status = f"🔴 Sin conexión: {str(e)[:50]}"

        await self._safe_reply(
            update,
            "🩺 *Diagnóstico del sistema*\n\n"
            f"*PostgreSQL:* {db_status}\n"
            f"• Fragmentos: {r['fragments']}\n\n"
            f"*Servicio de IA:* {ia_status}\n\n"
            f"*Modo debug:* {'🟢 ON' if DEBUG_MODE else '⚫ OFF'}\n"
            f"*Rate limit:* {RATE_LIMIT_MAX_REQUESTS} solicitudes/{RATE_LIMIT_WINDOW}s\n"
            f"*Timeout IA:* {REQUEST_TIMEOUT}s",
            parse_mode="Markdown"
        )


# ==================== MAIN ====================

async def main_async():
    loop = asyncio.get_running_loop()
    manager = None

    try:
        prompts = load_prompts()
        retriever = PostgresRetriever(DATABASE_URL, debug_mode=DEBUG_MODE)
        manager = BotManager(retriever, prompts=prompts)

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, manager.signal_handler)

        await asyncio.gather(
            retriever.connect(),
            manager.init_session(),
            return_exceptions=True
        )

        app = Application.builder().token(TOKEN).build()

        app.add_handler(CommandHandler("start", manager.start))
        app.add_handler(CommandHandler("help", manager.help))
        app.add_handler(CommandHandler("stats", manager.stats))
        app.add_handler(CommandHandler("diagnose", manager.diagnose))
        app.add_handler(CommandHandler("about", manager.about))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, manager.handle_message))

        logger.info("🤖 Bot YoguI A iniciado correctamente")
        logger.info("💡 Usa /diagnose para verificar el estado del sistema")

        async with app:
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)

            await manager.stop_event.wait()

            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    except Exception as e:
        logger.error("❌ Error fatal en main_async: %s", str(e))
        if DEBUG_MODE:
            import traceback
            logger.debug("Traceback: %s", traceback.format_exc())
        sys.exit(1)
    finally:
        if manager:
            await manager.close_resources()


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("👋 Bot detenido por el usuario")
    except Exception as e:
        logger.error("❌ Error fatal: %s", str(e))
        if DEBUG_MODE:
            import traceback
            logger.debug("Traceback: %s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
