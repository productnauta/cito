# Dicionário de Dados — CITO poc-v-d33

## `case_query`
- `_id`: ObjectId da execução de busca.
- `queryString`: termo pesquisado.
- `pageSize`: tamanho da página.
- `inteiroTeor`: booleano de busca em inteiro teor.
- `queryUrl`: URL da consulta.
- `htmlRaw`: HTML bruto retornado.
- `status`: `new`, `extracting`, `extracted`, `error`.
- `extractionTimestamp`: data/hora da coleta.
- `processedDate`: data/hora de término.
- `extractedCount`: quantidade de cards extraídos.

## `case_data`
- `identity`: identificação do processo.
- `identity.stfDecisionId`: ID STF.
- `identity.caseTitle`: título.
- `identity.caseClass`: classe.
- `identity.caseNumber`: número do processo.
- `identity.rapporteur`: relator.
- `identity.judgingBody`: órgão julgador.
- `dates.judgmentDate`: data de julgamento.
- `dates.publicationDate`: data de publicação.
- `caseContent.caseUrl`: URL do processo.
- `caseContent.caseHtml`: HTML completo.
- `caseContent.caseHtmlClean`: HTML principal limpo.
- `caseContent.raw.<secao>`: HTML sanitizado por seção.
- `caseContent.md.<secao>`: Markdown por seção.
- `caseData.caseParties`: partes.
- `caseData.caseKeywords`: palavras‑chave.
- `caseData.legislationReferences`: referências legislativas.
- `caseData.notesReferences`: referências de notas.
- `caseData.doctrineReferences`: citações doutrinárias.
- `caseData.decisionDetails`: detalhes extraídos da decisão.
- `caseData.decisionDetails.decisionResult.finalDecision`: decisão final em linguagem natural.
- `caseData.decisionDetails.ministerVotes[]`: votos por ministro (nome normalizado).
- `caseData.decisionDetails.speakers[]`: oradores e parte representada.
- `caseData.decisionDetails.partyRequests[]`: pedidos com parte solicitante, tipo, situação e descrição curta.
- `processing.*`: status e métricas por etapa.
- `status.pipelineStatus`: status consolidado.
- `audit.*`: timestamps de auditoria.

## `scrape_jobs`
- `_id`: ObjectId do job.
- `status`: `scheduled`, `running`, `completed`, `failed`, `canceled`.
- `scheduledFor`: data/hora programada.
- `query`: parâmetros da busca.
- `runId`: id de execução.
- `logPath`: caminho do log.

## `pipeline_jobs`
- `_id`: ObjectId do job.
- `caseQueryId`: referência ao `case_query`.
- `action`: `run` ou `reprocess`.
- `status`: `scheduled`, `running`, `completed`, `failed`, `canceled`.
- `runId`: id de execução.
- `logPath`: caminho do log.

## Normalização de nomes de ministros
- Campos afetados: `identity.rapporteur`, `identity.opinionWriter`, `caseIdentification.rapporteur`, `caseIdentification.opinionWriter`, `caseData.decisionDetails.ministerVotes[].ministerName`.
- Regra: remove `Min`/`Min.` (case-insensitive), normaliza espaços e aplica Title Case.
- Reprocessamento: `core/step10-normalize-minister-names.py`.
