# Scraping — CITO poc-v-d33

## Estratégia
- `step00-search-stf.py` usa Playwright para capturar HTML dos resultados de busca.
- `step02-get-case-html.py` usa requests com retries e fallback para Playwright em caso de challenge.

## WAF e desafios
- Detecção de challenge baseada em marcadores HTML.
- Em caso de challenge, o HTML é armazenado e o status é marcado como `challenge`.

## Configurações relevantes
- `config/query.yaml` define parâmetros da busca.
- `config/scrape.json` e `config/scraping.json` configuram timeouts, headers e fallback.

## Persistência
- HTML bruto de busca em `case_query.htmlRaw`.
- HTML completo do processo em `case_data.caseContent.caseHtml`.
