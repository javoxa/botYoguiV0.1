#!/usr/bin/env python3
"""
Punto de entrada para ejecutar el bot UNSA
"""
import sys
import os

# Asegurar que estamos en el directorio correcto
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Ejecutar el bot
from frontend.bot.telegram.telegram_bot_postgres import main

if __name__ == "__main__":
    main()
