#!/bin/bash

OUTPUT="proyecto_bot.txt"

# limpiar archivo de salida
> "$OUTPUT"

# encabezado general
printf "### ESTRUCTURA DEL PROYECTO: proyecto_unsa\n\n" >> "$OUTPUT"

tree -a -N --charset=ASCII \
  -I 'models|models--*|__pycache__|blobs|snapshots|*.safetensors|*.lock' \
  >> "$OUTPUT"


# buscar .py .txt .sql excluyendo modelos y cache
find . -type f \( \
        -name "*.py" -o \
        -name "*.txt" -o \
        -name "*.sql" \
    \) \
    ! -path "./backend/models/*" \
    ! -path "./backend/models--*/*" \
    ! -path "*/__pycache__/*" \
    | sort | while read -r archivo; do

        printf "\n########################################\n" >> "$OUTPUT"
        printf "### %s\n" "$archivo" >> "$OUTPUT"
        printf "########################################\n\n" >> "$OUTPUT"

        cat "$archivo" >> "$OUTPUT"
done
