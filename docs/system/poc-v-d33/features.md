# Funcionalidades — CITO poc-v-d33

## Produto (UI)
- Painel Doutrina com filtros, KPIs, gráficos e drill-down por autor, obra e relator.
- Painel Processos com filtros avançados e KPIs em tempo real.
- Painel Ministros com comparativos, métricas e detalhamento por ministro.
- Painel Scraping com agendamento, execução e acompanhamento de histórico.

## Backoffice (Pipeline)
- Coleta de HTML de resultados de busca do STF.
- Extração de cards e criação/atualização de processos em `case_data`.
- Coleta do HTML completo do processo com fallback para Playwright.
- Limpeza do HTML e extração de seções em HTML e Markdown.
- Extração de partes e palavras‑chave a partir do Markdown.
- Extrações via LLM para legislação, notas, doutrina e detalhes de decisão.

## Observações
- O pipeline é controlado por configurações em `config/pipeline.yaml`.
- Chamadas LLM usam prompts versionados em `config/prompts.yaml` e providers em `config/providers.yaml`.
