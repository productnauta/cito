# Diagrama de Fluxo de Campos por Step (case_data)

Este diagrama mostra como os campos evoluem ao longo dos steps do pipeline, destacando o que e criado/atualizado em cada etapa.

```mermaid
graph TD
  S0[Step00 - Orquestracao] --> S1[Step01 - Extracao de cards]
  S1 --> S2[Step02 - Scrape HTML]
  S2 --> S3[Step03 - Limpeza HTML]
  S3 --> S4[Step04 - Extracao de secoes]
  S4 --> S5[Step05 - Partes e keywords]
  S5 --> S6[Step06 - Legislacao]
  S6 --> S7[Step07 - Notes]
  S7 --> S8[Step08 - Doutrina]
  S8 --> S9[Step09 - Decision Details]

  S1 --- F1["case_data:\nidentity.*\naudit.*\nprocessing.pipelineStatus=extracted\nstatus.pipelineStatus=extracted"]
  S2 --- F2["caseContent.caseHtml\ncaseContent.caseUrl\nprocessing.caseScrape*\nprocessing.pipelineStatus=caseScraped\nstatus.pipelineStatus=caseScraped\naudit.updatedAt"]
  S3 --- F3["caseContent.caseHtmlClean\nprocessing.caseHtmlCleaningAt\nprocessing.caseHtmlCleanedAt\nprocessing.caseHtmlCleanMeta\nprocessing.pipelineStatus=caseHtmlCleaned\nstatus.pipelineStatus=caseHtmlCleaned\naudit.updatedAt"]
  S4 --- F4["caseContent.raw.<slug>\ncaseContent.md.<slug>\nprocessing.caseSectionsExtractingAt\nprocessing.caseSectionsExtractedAt\nprocessing.caseSectionsMeta\nprocessing.pipelineStatus=caseSectionsExtracted\nstatus.pipelineStatus=caseSectionsExtracted\naudit.updatedAt"]
  S5 --- F5["caseData.caseParties\ncaseData.caseKeywords\nprocessing.partiesKeywords.*\nprocessing.pipelineStatus=casePartiesKeywordsExtracted\nstatus.pipelineStatus=casePartiesKeywordsExtracted\naudit.updatedAt"]
  S6 --- F6["caseData.legislationReferences\nprocessing.caseLegislationRefs*\nprocessing.pipelineStatus=legislationExtracted\nstatus.pipelineStatus=legislationExtracted\naudit.updatedAt"]
  S7 --- F7["caseData.notesReferences\nprocessing.caseNotesRefs*\nprocessing.pipelineStatus=notesReferencesExtracted\nstatus.pipelineStatus=notesReferencesExtracted\naudit.updatedAt"]
  S8 --- F8["caseData.doctrineReferences\nprocessing.caseDoctrine*\nprocessing.pipelineStatus=doctrineExtracted\nstatus.pipelineStatus=doctrineExtracted\naudit.updatedAt"]
  S9 --- F9["caseData.decisionDetails\nprocessing.caseDecisionDetails*\nprocessing.pipelineStatus=decisionDetailsExtracted\nstatus.pipelineStatus=decisionDetailsExtracted\naudit.updatedAt"]
```

## Observacoes
- Campos `dates` e `caseTitle` aparecem como redundancias/atalhos para consulta e nao sao diretamente produzidos por um step especifico.
- Em caso de erro em cada step, os campos de `processing.*Error` e `processing.pipelineStatus` recebem o status de erro correspondente.
