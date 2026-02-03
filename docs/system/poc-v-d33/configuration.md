# Configuração — CITO poc-v-d33

## Arquivos em `config/`
- `mongo.yaml`: URI e database do MongoDB.
- `pipeline.yaml`: modo de execução e steps habilitados.
- `query.yaml`: parâmetros da busca no STF.
- `prompts.yaml`: templates para LLM.
- `providers.yaml`: providers e modelos LLM.
- `scrape.json`: opções de requests/playwright.
- `scraping.json`: estratégia detalhada de scraping.

## Observações
- Evite versionar credenciais em texto claro.
- Alterações em `pipeline.yaml` impactam a UI de scraping e o `step00-run-pipeline-02-09.py`.
