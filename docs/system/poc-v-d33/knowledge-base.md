# Base de Conhecimento — CITO poc-v-d33

## Propósito
Documento único, completo e detalhado da versão `poc-v-d33`, pensado como referência operacional e também como contexto para modelos de IA. Ele descreve o sistema, os fluxos, o modelo de dados, a interface web, o pipeline e as regras de extração.

## Escopo da versão
- Código fonte: `versions/development/poc-v-d33/`.
- Web app: `versions/development/poc-v-d33/web/`.
- Pipeline core: `versions/development/poc-v-d33/core/`.
- Configurações: `versions/development/poc-v-d33/config/`.
- Logs: `versions/development/poc-v-d33/core/logs/`.

## Visão Geral do Sistema
O CITO coleta decisões do STF, extrai metadados e conteúdo estruturado, e fornece uma interface web para análise. O processamento ocorre em etapas sequenciais (pipeline), gravando resultados no MongoDB. A UI consome o banco e permite navegação por processos, ministros, doutrina e acompanhamento de scraping.

## Arquitetura (alto nível)
- Web (Flask): rotas, templates HTML e CSS. Produz páginas analíticas com filtros e gráficos.
- Core (scripts): etapas do pipeline, scraping e extrações baseadas em LLM.
- MongoDB: persistência de resultados brutos e estruturados.
- LLMs: extração de doutrina, legislação, notas e detalhes de decisão.
- Scraping: coleta via requests/Playwright com fallback e detecção de challenge.

## Fluxo de Dados Principal
1. `step00-search-stf.py` coleta HTML da busca e grava em `case_query`.
2. `step01-extract-cases.py` extrai cards e upserta em `case_data`.
3. `step02-get-case-html.py` baixa HTML completo do processo.
4. `step03-clean-case-html.py` limpa HTML e extrai conteúdo principal.
5. `step04-extract-sessions.py` separa seções e gera Markdown.
6. `step05-extract-keywords-parties.py` estrutura partes e palavras‑chave.
7. `step06-extract-legislation-mistral.py` extrai legislação via LLM.
8. `step07-extract-notes-mistral.py` extrai notas via LLM.
9. `step08-doctrine-mistral.py` extrai doutrina via LLM.
10. `step09-extract-decision-details-mistral.py` extrai detalhes da decisão via LLM.

## Componentes e Pastas
- `web/app.py`: servidor Flask e rotas.
- `web/templates/`: páginas HTML.
- `web/static/styles.css`: estilos.
- `core/step00-*.py`: orquestração e scraping.
- `core/step01-*.py` a `core/step09-*.py`: etapas do pipeline.
- `core/utils/mongo.py`: acesso ao MongoDB.
- `config/*.yaml`: configuração de providers, prompts, pipeline e busca.

## Modelo de Dados
### Coleções
- `case_query`: execuções de busca (HTML bruto e metadados de consulta).
- `case_data`: processos estruturados e enriquecidos.
- `scrape_jobs`: agendamentos de scraping pela UI.
- `pipeline_jobs`: execuções de pipeline acionadas pela UI.

### `case_query` (principais campos)
- `queryString`: termo de busca.
- `pageSize`: tamanho da página.
- `inteiroTeor`: booleano de busca em inteiro teor.
- `queryUrl`: URL de consulta.
- `htmlRaw`: HTML bruto.
- `status`: `new`, `extracting`, `extracted`, `error`.
- `extractionTimestamp`: data/hora da coleta.
- `processedDate`: data/hora de término.
- `extractedCount`: número de cards extraídos.

### `case_data` (principais campos)
- `identity.*`: identificadores e metadados do processo.
- `dates.*`: datas relevantes (julgamento, publicação).
- `caseContent.*`: HTML bruto, HTML limpo e seções em HTML/Markdown.
- `caseData.*`: dados estruturados (partes, palavras‑chave, doutrina, legislação, notas e decisão).
- `processing.*`: status e métricas por etapa.
- `status.pipelineStatus`: status consolidado da pipeline.
- `audit.*`: timestamps de auditoria.

### Campos relevantes do `identity`
- `identity.stfDecisionId`: identificador canônico do processo (usado em links internos).
- `identity.caseTitle`: título do processo.
- `identity.caseClass`: classe processual.
- `identity.caseNumber`: número do processo.
- `identity.rapporteur`: relator.
- `identity.judgingBody`: órgão julgador.
- `identity.caseUrl`: URL externa do STF.

### Seções geradas em `caseContent`
- `caseContent.caseHtml`: HTML completo do processo.
- `caseContent.caseHtmlClean`: HTML principal limpo.
- `caseContent.raw.<secao>`: HTML sanitizado por seção.
- `caseContent.md.<secao>`: Markdown por seção.

### Estruturas de extração LLM
- `caseData.legislationReferences`: normas e dispositivos (protocolo N/R).
- `caseData.notesReferences`: notas extraídas (CITO-REF/1).
- `caseData.doctrineReferences`: doutrina (CITO-DOCTRINE/1).
- `caseData.decisionDetails`: detalhes da decisão (JSON).

## Pipeline (detalhe por etapa)
### Step00 — `step00-search-stf.py`
- Entrada: `config/query.yaml`.
- Saída: `case_query` com `htmlRaw`.

### Step01 — `step01-extract-cases.py`
- Entrada: `case_query.htmlRaw`.
- Saída: `case_data` com `identity`, `dates`, `caseContent.caseUrl`.

### Step02 — `step02-get-case-html.py`
- Entrada: `identity.stfDecisionId`.
- Saída: `caseContent.caseHtml`.
- Fallback: Playwright se detectar challenge.

### Step03 — `step03-clean-case-html.py`
- Entrada: `caseContent.caseHtml`.
- Saída: `caseContent.caseHtmlClean`.

### Step04 — `step04-extract-sessions.py`
- Entrada: `caseContent.caseHtmlClean`.
- Saída: `caseContent.raw.*` e `caseContent.md.*`.

### Step05 — `step05-extract-keywords-parties.py`
- Entrada: `caseContent.md.parties` e `caseContent.md.keywords`.
- Saída: `caseData.caseParties` e `caseData.caseKeywords`.

### Step06 — `step06-extract-legislation-mistral.py`
- Entrada: `caseContent.md.legislation`.
- Saída: `caseData.legislationReferences`.

### Step07 — `step07-extract-notes-mistral.py`
- Entrada: `caseContent.md.notes`.
- Saída: `caseData.notesReferences`.

### Step08 — `step08-doctrine-mistral.py`
- Entrada: `caseContent.md.doctrine`.
- Saída: `caseData.doctrineReferences`.

### Step09 — `step09-extract-decision-details-mistral.py`
- Entrada: `caseContent.md.decision`.
- Saída: `caseData.decisionDetails`.

## Protocolos de Extração (LLM)
### Legislação — protocolo N/R
- Linhas `N|...` definem a norma.
- Linhas `R|...` definem dispositivos (artigos, incisos, parágrafos).

### Notas — protocolo CITO-REF/1
- Blocos com linhas `H|`, `L|`, `I|`, `M|`.
- Cada linha `I|` gera item dentro de uma nota.

### Doutrina — protocolo CITO-DOCTRINE/1
- Linhas `C|author|publicationTitle|edition|publicationPlace|publisher|year|page|rawCitation`.

### Decisão — JSON
- `decisionSummary`, `decisionResult`, `ministerVotes`, `partyRequests`, `speakers`, `legalBasis`, `citations`, `decisionType`, `decisionDates`.

## Interface Web (rotas)
- `/doutrina`: painel de doutrina.
- `/doutrina/detalhe`: detalhe por autor/obra/relator.
- `/processos`: painel de processos.
- `/processos/<process_id>`: detalhe do processo.
- `/ministros`: painel de ministros.
- `/ministros/detalhe`: detalhe do ministro.
- `/scraping`: painel de scraping.
- `/scraping/<run_id>`: detalhe de execução.

## Funcionalidades de UI
- Filtros por autor, título, classe, relator, datas.
- KPIs e gráficos com Chart.js.
- Tabelas com navegação por entidades.
- Ações de scraping e pipeline via UI.

## Regras de Navegação (links contextuais)
- Títulos de processo devem linkar para `/processos/<stfDecisionId>`.
- Nomes de relatores/ministros devem linkar para `/ministros/detalhe`.
- Autores e obras devem linkar para `doutrina/detalhe` com `kind=author` ou `kind=title`.
- Quando não houver identificador válido, o texto não deve ser clicável.

## Configurações
- `config/mongo.yaml`: conexão com MongoDB.
- `config/pipeline.yaml`: steps, delays e modo de execução.
- `config/query.yaml`: parâmetros de busca do STF.
- `config/prompts.yaml`: templates dos prompts.
- `config/providers.yaml`: providers e modelos LLM.
- `config/scrape.json` e `config/scraping.json`: parâmetros de scraping.

## Logs
- Diretório: `core/logs/`.
- Ações da UI: `web-actions.log` com prefixo de timestamp.
- Execuções de scraping e pipeline: arquivos `AAAAMMDD-HHMMSS-*.log`.

## Operação e Execução
- Web app: execute `web/app.py`.
- Pipeline manual: execute steps individualmente ou via `step00-run-pipeline-02-09.py`.
- Scraping manual: execute `step00-search-stf.py`.

## Limitações conhecidas
- Sem autenticação na UI.
- Execução via `subprocess` pode bloquear requests longas.
- Dependência direta de LLMs externos.

## Segurança
- Credenciais em `config/mongo.yaml` e `config/providers.yaml` devem ser movidas para variáveis de ambiente.
- Evitar exposição de logs com dados sensíveis.

## Glossário
- `case_query`: coleção com HTML bruto da busca.
- `case_data`: coleção com processos enriquecidos.
- `stfDecisionId`: identificador canônico do processo.
- `pipelineStatus`: status consolidado das etapas.
- `CITO-REF/1`: protocolo de notas.
- `CITO-DOCTRINE/1`: protocolo de doutrina.
- `N/R`: protocolo de normas e dispositivos.
