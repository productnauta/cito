# Visão Geral — CITO poc-v-d33

## Objetivo
Sistema para coletar decisões do STF, estruturar metadados e permitir exploração analítica por doutrina, processos e ministros via interface web.

## Escopo
- Coleta de resultados de busca do STF e persistência em MongoDB.
- Pipeline de enriquecimento por etapas para HTML, seções, palavras-chave, partes, legislação, notas, doutrina e detalhes de decisão.
- Interface web para exploração e monitoramento de scraping/pipeline.

## Fora de escopo
- Autenticação/controle de acesso.
- APIs públicas ou integrações externas além de LLMs e scraping.
- Orquestração distribuída (fila/worker).

## Versão documentada
- Código base: `versions/development/poc-v-d33`.
- Aplicação web: `versions/development/poc-v-d33/web/app.py`.
- Pipeline core: `versions/development/poc-v-d33/core/`.
- Configs: `versions/development/poc-v-d33/config/`.
