# Operação — CITO poc-v-d33

## Execução local
- A aplicação web é executada por `web/app.py`.
- Steps individuais podem ser executados manualmente via CLI.

## Logs
- Pipeline grava logs em `core/logs/`.
- Ações da UI são registradas em `core/logs/web-actions.log`.

## Troubleshooting
- Falhas de scraping podem ser causadas por WAF, tempo de resposta ou SSL.
- Falhas de LLM podem ocorrer por timeout, erro HTTP ou formato inválido.

## Monitoramento
- Consulte `processing.*` e `status.pipelineStatus` no `case_data`.
- Consulte `scrape_jobs` e `pipeline_jobs` para filas.
