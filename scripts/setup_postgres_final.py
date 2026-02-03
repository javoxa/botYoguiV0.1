#!/usr/bin/env python3
"""
Setup final para PostgreSQL - Versión que SÍ funciona
"""
import asyncpg
import asyncio
import sys
from pathlib import Path

async def setup_postgres():
    print("🗄️  Configuración PostgreSQL para UNSA - Versión Final")
    print("=" * 60)

    # Configuración de conexión
    db_config = {
        "host": os.getenv("DB_HOST","localhost"),#poner la IP del CPU
        "database": "unsa_knowledge_db",
        "user": "unsa_admin",
        "password": "unsa_password"
    }

    try:
        # 1. Conectar
        conn = await asyncpg.connect(**db_config)
        print("✅ Conectado a PostgreSQL como 'unsa_admin'")

        # 2. Crear esquema dedicado
        await conn.execute("CREATE SCHEMA IF NOT EXISTS unsa_esquema")
        print("✅ Esquema 'unsa_esquema' creado")

        # 3. Dar permisos en el esquema
        await conn.execute("GRANT ALL ON SCHEMA unsa_esquema TO unsa_admin")
        print("✅ Permisos otorgados en el esquema")

        # 4. Establecer search_path
        await conn.execute("ALTER USER unsa_admin SET search_path TO unsa_esquema, public")
        print("✅ search_path configurado")

        # 5. Cambiar al esquema para las siguientes operaciones
        await conn.execute("SET search_path TO unsa_esquema")

        # 6. Leer y ejecutar migración
        migration_file = Path("database/migrations/migration_001_initial_fixed.sql")
        if migration_file.exists():
            print(f"📄 Leyendo migración: {migration_file.name}")
            sql = migration_file.read_text()

            # Dividir por sentencias SQL (separadas por ';')
            statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]

            for i, stmt in enumerate(statements, 1):
                try:
                    await conn.execute(stmt)
                    print(f"  [{i}/{len(statements)}] ✓")
                except Exception as e:
                    # Ignorar errores de "ya existe" y continuar
                    if "already exists" not in str(e) and "ya existe" not in str(e):
                        print(f"  [{i}/{len(statements)}] ⚠️  {str(e)[:80]}...")

            print("✅ Migración aplicada")
        else:
            print("⚠️  Archivo de migración no encontrado, creando estructura básica...")
            await create_basic_schema(conn)

        # 7. Verificar
        print("\n📊 VERIFICACIÓN:")

        # Tablas creadas
        tables = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'unsa_esquema'
            ORDER BY table_name
        """)
        print(f"  • Tablas creadas: {len(tables)}")
        for table in tables[:5]:  # Mostrar primeras 5
            print(f"    - {table['table_name']}")
        if len(tables) > 5:
            print(f"    ... y {len(tables) - 5} más")

        # Conteo de fragmentos
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM fragmentos_conocimiento")
            print(f"  • Fragmentos de conocimiento: {count}")
        except:
            print("  • Fragmentos: Tabla no disponible aún")

        await conn.close()

        print("\n" + "=" * 60)
        print("✅ CONFIGURACIÓN COMPLETADA EXITOSAMENTE")
        print("=" * 60)
        print("\n📋 Próximos pasos:")
        print("1. Iniciar servidor de inferencia:")
        print("   python3 backend/inference_server.py")
        print("\n2. Iniciar bot escalable:")
        print("   python3 frontend/telegram_bot_scalable.py")

    except asyncpg.InvalidCatalogNameError:
        print("❌ ERROR: La base de datos 'unsa_knowledge_db' no existe")
        print("\n💡 Crea la base de datos con este comando (necesitas acceso temporal a postgres):")
        print("""
# Si puedes usar sudo temporalmente:
sudo -u postgres createdb unsa_knowledge_db

# O pide a un administrador que ejecute:
createdb unsa_knowledge_db
""")
        sys.exit(1)

    except asyncpg.InsufficientPrivilegeError as e:
        print(f"❌ ERROR DE PERMISOS: {e}")
        print("\n💡 Solución temporal (ejecuta en psql como usuario con privilegios):")
        print("""
-- Conéctate como usuario con privilegios (ej: postgres) y ejecuta:
GRANT ALL PRIVILEGES ON DATABASE unsa_knowledge_db TO unsa_admin;
\c unsa_knowledge_db
GRANT ALL ON SCHEMA public TO unsa_admin;
""")
        print("\n💡 Solución permanente (usa el esquema dedicado):")
        print("Ejecuta este script nuevamente después de otorgar permisos básicos.")
        sys.exit(1)

    except Exception as e:
        print(f"❌ ERROR INESPERADO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

async def create_basic_schema(conn):
    """Crear esquema básico si no hay migración"""
    print("🏗️  Creando estructura básica...")

    # Tabla simple de conocimiento
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS fragmentos_conocimiento (
            id SERIAL PRIMARY KEY,
            contenido TEXT NOT NULL,
            categoria VARCHAR(100),
            facultad VARCHAR(100),
            palabras_clave TEXT[],
            usado_count INTEGER DEFAULT 0,
            fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insertar datos básicos
    datos = [
        ("UNSA Universidad Nacional de Salta", "General", "UNSA"),
        ("Preinscripciones 2026: 1 al 30 de septiembre", "Inscripción", "General"),
        ("Carrera de Medicina 7 años", "Carrera", "Salud"),
        ("Ingeniería en Informática - Facultad de Ciencias Exactas", "Carrera", "Exactas"),
        ("Becas de ayuda económica", "Beca", "General"),
    ]

    for contenido, categoria, facultad in datos:
        await conn.execute("""
            INSERT INTO fragmentos_conocimiento (contenido, categoria, facultad)
            VALUES ($1, $2, $3)
        """, contenido, categoria, facultad)

    print(f"✅ {len(datos)} registros insertados")

if __name__ == "__main__":
    asyncio.run(setup_postgres())
