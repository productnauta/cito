# Arquitetura — CITO poc-v-d33

## Componentes
- Web: Flask em `web/app.py` com templates em `web/templates/` e CSS em `web/static/`.
- Core: scripts de pipeline em `core/step00-...step09...py`.
- Banco de dados: MongoDB com coleções `case_query`, `case_data`, `scrape_jobs`, `pipeline_jobs`.
- LLMs: chamadas HTTP para Mistral (e potencial Groq), com prompts YAML.
- Scraping: requests e Playwright com fallback e detecção de WAF.

## Fluxo principal de dados
1. `step00-search-stf.py` coleta HTML de busca e cria documento em `case_query`.
2. `step01-extract-cases.py` parseia resultados e upserta em `case_data`.
3. `step02-get-case-html.py` baixa HTML completo do processo.
4. `step03-clean-case-html.py` extrai conteúdo principal.
5. `step04-extract-sessions.py` separa seções e gera Markdown.
6. `step05-extract-keywords-parties.py` estrutura partes e palavras‑chave.
7. `step06/07/08/09` fazem extrações via LLM.

## Controle e monitoramento
- `web/app.py` orquestra execução de scraping e pipeline via `subprocess`.
- Status do pipeline são gravados em `processing.*` e `status.pipelineStatus`.
- Logs são gravados em `core/logs/`.
