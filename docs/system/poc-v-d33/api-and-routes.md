# Rotas e Endpoints — CITO poc-v-d33

## Rotas Web
- `/` → redireciona para `/doutrina`.
- `/doutrina` → painel de doutrina.
- `/doutrina/detalhe` → detalhe por autor/obra/relator.
- `/processos` → painel de processos.
- `/processos/<process_id>` → detalhe do processo.
- `/ministros` → painel de ministros.
- `/ministros/detalhe` → detalhe do ministro.
- `/scraping` → painel de scraping.
- `/scraping/<run_id>` → detalhe de execução.

## Rotas de ação
- `POST /scraping/schedule` → cria job.
- `POST /scraping/cancel/<job_id>` → cancela job.
- `POST /scraping/execute/<job_id>` → executa job.
- `POST /scraping/<run_id>/pipeline/run` → executa pipeline.
- `POST /scraping/<run_id>/pipeline/reprocess` → reprocessa.
- `POST /scraping/<run_id>/pipeline/cancel` → cancela pipeline.

## Endpoint JSON
- `/processos/kpis` → KPIs com filtros aplicados.
