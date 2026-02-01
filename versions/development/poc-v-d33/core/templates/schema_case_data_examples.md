# Exemplos Reais de Documentos: `case_data`

Este documento apresenta exemplos reais (recortados) dos documentos `case_data`, com foco nos campos relevantes para consulta, IA e auditoria. Os trechos foram extraidos dos arquivos:
- `versions/development/poc-v-d33/core/templates/json-doc-1.json`
- `versions/development/poc-v-d33/core/templates/json-doc-2.json`
- `versions/development/poc-v-d33/core/templates/json-doc-3.json`

## 1. Identificacao e auditoria

```json
{
  "identity": {
    "stfDecisionId": "sjur452406",
    "caseTitle": "ADI 6476",
    "caseUrl": "https://jurisprudencia.stf.jus.br/pages/search/sjur452406/false",
    "caseClass": "ADI",
    "caseNumber": "6476",
    "judgingBody": "Tribunal Pleno",
    "rapporteur": "Tribunal Pleno",
    "domClipboardId": "clipboard-1"
  },
  "audit": {
    "extractionDate": {"$date": "2026-02-01T12:50:03.734Z"},
    "lastExtractedAt": {"$date": "2026-02-01T12:50:04.331Z"},
    "builtAt": {"$date": "2026-02-01T12:50:03.734Z"},
    "updatedAt": {"$date": "2026-02-01T13:16:33.254Z"},
    "sourceStatus": "extracted",
    "pipelineStatus": "extracted"
  }
}
```

## 2. Conteudo e secoes extraidas

```json
{
  "caseContent": {
    "caseUrl": "https://jurisprudencia.stf.jus.br/pages/search/sjur452406/false",
    "caseHtml": "<!DOCTYPE html>...",
    "caseHtmlClean": "<div class=...>",
    "md": {
      "header": "#### ADI 6476 / DF...",
      "summary": "EMENTA: Direito Constitucional...",
      "decision": "O Tribunal, por unanimidade...",
      "keywords": "- CONSTITUICAO FEDERAL...",
      "legislation": "LEG-FED CF ANO-1988...",
      "notes": "- Acordao(s) citado(s)...",
      "doctrine": "SUNSTEIN, Cass. Cost-benefit..."
    },
    "raw": {
      "header": "<h4>ADI 6476...</h4>",
      "summary": "EMENTA: Direito Constitucional...",
      "decision": "O Tribunal, por unanimidade..."
    }
  }
}
```

## 3. Dados enriquecidos (`caseData`)

### 3.1. Palavras-chave e partes

```json
{
  "caseData": {
    "caseKeywords": ["CONSTITUICAO FEDERAL", "DEVER DE PROTECAO"],
    "caseParties": [
      {"partieType": "REQTE.(S)", "partieName": "PARTIDO SOCIALISTA BRASILEIRO - PSB"},
      {"partieType": "ADV.(A/S)", "partieName": "RAFAEL DE ALENCAR ARARIPE CARNEIRO"}
    ]
  }
}
```

### 3.2. Legislacao referenciada

```json
{
  "caseData": {
    "legislationReferences": [
      {
        "normIdentifier": "CF-1988",
        "jurisdictionLevel": "federal",
        "normType": "OUTRA",
        "normYear": 1988,
        "normDescription": "CONSTITUICAO FEDERAL",
        "normReferences": [
          {"articleNumber": 5, "isCaput": true, "incisoNumber": 3}
        ]
      }
    ]
  }
}
```

### 3.3. Notas referenciadas

```json
{
  "caseData": {
    "notesReferences": [
      {
        "noteType": "stf_acordao",
        "rawLine": "ADI 4788 AgR (TP)",
        "items": [
          {
            "itemType": "decision",
            "caseClass": "ADI",
            "caseNumber": "4788",
            "suffix": "AgR",
            "orgTag": "TP",
            "country": null,
            "rawRef": "ADI 4788 AgR (TP)"
          }
        ]
      }
    ]
  }
}
```

### 3.4. Doutrina referenciada

```json
{
  "caseData": {
    "doctrineReferences": [
      {
        "author": "SUNSTEIN, Cass",
        "publicationTitle": "Cost-benefit analysis without analyzing costs of benefits...",
        "edition": null,
        "publicationPlace": null,
        "publisher": null,
        "year": 2007,
        "page": "1895-1909",
        "rawCitation": "SUNSTEIN, Cass. Cost-benefit analysis..."
      }
    ]
  }
}
```

### 3.5. Detalhes da decisao

```json
{
  "caseData": {
    "decisionDetails": {
      "decisionSummary": {"summary": "O Tribunal converteu..."},
      "decisionResult": {"finalDecision": "procedente"},
      "ministerVotes": [
        {"ministerName": null, "voteType": "favoravel", "voteSummary": "Voto do Relator..."}
      ],
      "partyRequests": [
        {"requestType": "merito", "requestingParty": null, "requestDescription": "Conhecimento...", "requestOutcome": "atendido"}
      ],
      "speakers": [
        {"lawyerName": "Felipe Santos Correa", "representedParty": "requerente", "argumentSummary": null}
      ],
      "legalBasis": {"summary": "Interpretacao conforme a Constituicao..."},
      "citations": [
        {"citationType": "legislacao", "citationName": "Decreto no 9.508/2018", "relevanceSummary": "Dispositivos questionados..."}
      ],
      "decisionType": "plenaria",
      "decisionDates": {"judgmentStartDate": "2021-08-27", "judgmentEndDate": "2021-09-03"}
    }
  }
}
```

## 4. Metadados de processamento

```json
{
  "processing": {
    "pipelineStatus": "decisionDetailsExtracted",
    "caseScrapeStatus": "success",
    "caseScrapeAt": {"$date": "2026-02-01T13:15:33.408Z"},
    "caseHtmlCleaningAt": {"$date": "2026-02-01T13:15:38.966Z"},
    "caseHtmlCleanedAt": {"$date": "2026-02-01T13:15:39.111Z"},
    "caseSectionsExtractedAt": {"$date": "2026-02-01T13:15:44.611Z"},
    "caseLegislationRefsStatus": "success",
    "caseNotesRefsStatus": "success",
    "caseDoctrineStatus": "success",
    "caseDecisionDetailsStatus": "success"
  }
}
```

## 5. Datas normalizadas

```json
{
  "caseTitle": "ADI 6476",
  "dates": {
    "judgmentDate": "08/09/2021",
    "publicationDate": "08/09/2021"
  }
}
```
