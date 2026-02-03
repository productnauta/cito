# Modelo de Dados — CITO poc-v-d33

## Coleções
- `case_query`: registros brutos de consultas ao STF.
- `case_data`: processos extraídos e enriquecidos.
- `scrape_jobs`: agendamentos de scraping via UI.
- `pipeline_jobs`: execuções de pipeline acionadas via UI.

## Entidades principais
- Caso/Processo: `case_data`.
- Consulta de busca: `case_query`.
- Job de scraping: `scrape_jobs`.
- Job de pipeline: `pipeline_jobs`.

## Relacionamentos
- `case_data.identity.caseQueryId` referencia `_id` de `case_query`.
- `scrape_jobs.caseQueryId` pode referenciar uma execução gerada.
- `pipeline_jobs.caseQueryId` referencia a execução de `case_query`.
