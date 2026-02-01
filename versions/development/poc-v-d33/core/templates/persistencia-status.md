# Persistencia de Status e Etapas (case_data)

Este documento descreve os requisitos, condicoes e ocorrencias usados para persistir status, execucoes e etapas no MongoDB, conforme implementado nos scripts de `versions/development/poc-v-d33/core`.

## Regras gerais

- Sempre atualizar `audit.updatedAt` em qualquer gravacao relevante.
- Metadados da etapa vivem em `processing.*` (status, erro, timestamps, provider/model, latencia, contagens).
- `status.pipelineStatus` representa o ultimo estado “oficial” da pipeline.
- Em sucesso, normalmente alinhar `processing.pipelineStatus` e `status.pipelineStatus`.

## Step01 - Extracao de cards (case_query -> case_data)

### case_query
- Claim atomico: `status` de `new` -> `extracting` + `extractingAt`.
- Sucesso: `status=extracted`, `processedDate`, `extractedCount`.
- Erro: `status=error`, `processedDate`, `error`.

### case_data (upsert por identity.stfDecisionId)
- Preencher `audit.extractionDate`, `audit.lastExtractedAt`, `audit.builtAt`, `audit.updatedAt`.
- `audit.sourceStatus="extracted"`, `audit.pipelineStatus="extracted"`.
- `processing.pipelineStatus="extracted"`.
- Em update: sempre setar `audit.updatedAt` e `audit.lastExtractedAt`.
- Em insert: garantir `audit.builtAt`.

## Step02 - Obter HTML bruto

### Sucesso
- `caseContent.caseHtml`, `caseContent.caseUrl`.
- `processing.caseScrapeStatus="success"`.
- `processing.caseScrapeError=null`.
- `processing.caseScrapeAt`, `processing.caseScrapeHttpStatus`,
  `processing.caseScrapeLatencyMs`, `processing.caseScrapeHtmlBytes`.
- `processing.pipelineStatus="caseScraped"`.
- `status.pipelineStatus="caseScraped"`.
- `audit.updatedAt`.
- Em insert: `audit.createdAt`.

### Erro
- `processing.caseScrapeStatus="error"`.
- `processing.caseScrapeError=<mensagem>`.
- `processing.caseScrapeAt`.
- `processing.pipelineStatus="caseScrapeError"`.
- `audit.updatedAt`.
- **Nao** atualizar `status.pipelineStatus`.

### Challenge (requires JS)
- `processing.caseScrapeStatus="challenge"`.
- `processing.caseScrapeChallenge=true`.
- `processing.caseScrapeChallengeHtml` (truncado).
- `processing.caseScrapeHttpStatus`, `processing.caseScrapeLatencyMs`,
  `processing.caseScrapeAt`.
- `processing.pipelineStatus="caseScrapeChallenge"`.
- `audit.updatedAt`.
- **Nao** atualizar `status.pipelineStatus`.

## Step03 - Limpar HTML

### Sucesso
- `caseContent.caseHtmlClean`.
- `processing.caseHtmlCleanedAt`, `processing.caseHtmlCleanMeta`,
  `processing.caseHtmlCleanError=null`.
- `processing.pipelineStatus="caseHtmlCleaned"`.
- `status.pipelineStatus="caseHtmlCleaned"`.
- `audit.updatedAt`, `audit.lastCaseHtmlCleanedAt`.

### Erro
- `processing.caseHtmlCleanedAt`.
- `processing.caseHtmlCleanError=<mensagem>`.
- `processing.pipelineStatus="caseHtmlCleanError"`.
- `audit.updatedAt`.

## Step04 - Extrair secoes

### Sucesso
- `caseContent.raw.<slug>` e `caseContent.md.<slug>`.
- `processing.caseSectionsExtractedAt`, `processing.caseSectionsMeta`,
  `processing.caseSectionsError=null`.
- `processing.pipelineStatus="caseSectionsExtracted"`.
- `status.pipelineStatus="caseSectionsExtracted"`.
- `audit.updatedAt`, `audit.lastSectionsExtractedAt`.

### Erro
- `processing.caseSectionsExtractedAt`.
- `processing.caseSectionsError=<mensagem>`.
- `processing.pipelineStatus="caseSectionsExtractError"`.
- `audit.updatedAt`.

## Step05 - Partes e palavras-chave

### Sucesso
- `caseData.caseParties`, `caseData.caseKeywords`.
- `processing.partiesKeywords.finishedAt`,
  `processing.partiesKeywords.partiesCount`,
  `processing.partiesKeywords.keywordsCount`.
- `processing.pipelineStatus="casePartiesKeywordsExtracted"`.
- `status.pipelineStatus="casePartiesKeywordsExtracted"`.
- `audit.updatedAt`, `status.updatedAt`.

### Erro
- `processing.pipelineStatus="casePartiesKeywordsExtractError"`.
- `status.pipelineStatus="casePartiesKeywordsExtractError"`.
- `status.error=<mensagem>`, `status.updatedAt`.

## Step06 - Legislacao (Python)

### Sucesso
- `caseData.legislationReferences`.
- `processing.caseLegislationRefsStatus="success"`.
- `processing.caseLegislationRefsError=null`.
- `processing.caseLegislationRefsAt`.
- `processing.caseLegislationRefsProvider="python"`.
- `processing.caseLegislationRefsModel="regex"`.
- `processing.pipelineStatus="legislationExtracted"`.
- `status.pipelineStatus="legislationExtracted"`.
- `audit.updatedAt`, `status.updatedAt`.

### Erro
- `processing.caseLegislationRefsStatus="error"`.
- `processing.caseLegislationRefsError=<mensagem>`.
- `processing.caseLegislationRefsAt`.
- `processing.caseLegislationRefsProvider="python"`.
- `processing.caseLegislationRefsModel="regex"`.
- `processing.pipelineStatus="legislationExtractError"`.
- `status.pipelineStatus="legislationExtractError"`.
- `audit.updatedAt`, `status.updatedAt`.

## Step06 - Legislacao (Groq)

### Sucesso
- `caseData.legislationReferences`.
- `processing.caseLegislationRefsStatus="success"`.
- `processing.caseLegislationRefsError=null`.
- `processing.caseLegislationRefsAt`.
- `processing.caseLegislationRefsProvider="groq"`.
- `processing.caseLegislationRefsModel=<model>`.
- `processing.caseLegislationRefsLatencyMs`.
- `processing.pipelineStatus="legislationExtracted"`.
- `status.pipelineStatus="legislationExtracted"`.
- `audit.updatedAt`.

### Erro
- `processing.caseLegislationRefsStatus="error"`.
- `processing.caseLegislationRefsError=<mensagem>`.
- `processing.caseLegislationRefsAt`.
- `processing.caseLegislationRefsProvider="groq"`.
- `processing.caseLegislationRefsModel=<model>`.
- `processing.pipelineStatus="legislationExtractError"`.
- `status.pipelineStatus="legislationExtractError"`.
- `audit.updatedAt`.

## Step07 - Notes (Groq)

### Sucesso
- `caseData.notesReferences`.
- `processing.caseNotesRefsStatus="success"`.
- `processing.caseNotesRefsError=null`.
- `processing.caseNotesRefsAt`.
- `processing.caseNotesRefsProvider="groq"`.
- `processing.caseNotesRefsModel=<model>`.
- `processing.caseNotesRefsLatencyMs`.
- `processing.pipelineStatus="notesReferencesExtracted"`.
- `status.pipelineStatus="notesReferencesExtracted"`.
- `audit.updatedAt`.

### Erro
- `processing.caseNotesRefsStatus="error"`.
- `processing.caseNotesRefsError=<mensagem>`.
- `processing.caseNotesRefsAt`.
- `processing.caseNotesRefsProvider="groq"`.
- `processing.caseNotesRefsModel=<model>`.
- `processing.pipelineStatus="notesReferencesExtractError"`.
- `status.pipelineStatus="notesReferencesExtractError"`.
- `audit.updatedAt`.

## Step08 - Doutrina (Groq)

### Sucesso
- `caseData.caseDoctrines`.
- `processing.caseDoctrineStatus="success"`.
- `processing.caseDoctrineError=null`.
- `processing.caseDoctrineAt`.
- `processing.caseDoctrineProvider="groq"`.
- `processing.caseDoctrineModel=<model>`.
- `processing.caseDoctrineLatencyMs`.
- `processing.caseDoctrineCount`.
- `processing.pipelineStatus="doctrineExtracted"`.
- `status.pipelineStatus="doctrineExtracted"`.
- `audit.updatedAt`.

### Erro
- `processing.caseDoctrineStatus="error"`.
- `processing.caseDoctrineError=<mensagem>`.
- `processing.caseDoctrineAt`.
- `processing.caseDoctrineProvider="groq"`.
- `processing.caseDoctrineModel=<model>`.
- `processing.pipelineStatus="doctrineExtractError"`.
- `status.pipelineStatus="doctrineExtractError"`.
- `audit.updatedAt`.

## Condicoes de gating entre etapas

- Step03 processa apenas documentos com `status.pipelineStatus="caseScraped"`.
- Step04 processa apenas documentos com `status.pipelineStatus="caseHtmlCleaned"`.
- Step00 executa pipeline apenas quando `processing.caseScrapeStatus` esta ausente/nulo/vazio.
