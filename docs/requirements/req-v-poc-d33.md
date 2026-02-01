# CITO Project — System Requirements Specification

## 1. Document Control

* **Project**: CITO
* **Domain**: Jurisprudência do STF (Supremo Tribunal Federal)
* **Version**: v-d33
* **Start Date**: 2025-01-20
* **Document Type**: System Requirements Specification (SRS)
* **Primary Identifier Rule**: `identity.stfDecisionId`

---

## 2. Purpose and Scope

This document defines the complete functional and technical requirements of the CITO Project. The system is responsible for collecting, sanitizing, structuring, and enriching jurisprudential data from the STF website, transforming unstructured HTML content into structured, auditable, and semantically usable data stored in MongoDB.

The specification covers all processing stages, from search-result extraction to semantic enrichment, including data schemas, pipeline rules, status transitions, and extraction patterns.

---

## 3. Global Rules and Conventions

### 3.1 Canonical Process Identifier

* The **only valid process identifier** across the entire system is:

```
identity.stfDecisionId
```

* This identifier must be used in:

  * filters
  * CLI inputs
  * logs
  * pipeline execution
  * auditing

### 3.2 Collections Overview

* `case_query`: stores HTML of STF search result pages
* `case_data`: stores unified and enriched process documents

### 3.3 Status Management

* `status.pipelineStatus`: logical pipeline state
* `processing`: technical execution metadata
* `audit`: historical traceability of actions

---

## 4. Pipeline 1 — Identify and Extract Processes (Search Results)

### 4.1 Objective

Identify individual processes from STF search-result pages and create or update base documents in `case_data`.

### 4.2 Input

* Collection: `case_query`
* Field: `htmlRaw`
* Filter:

```
pipelineStatus = "new"
```

### 4.3 Processing Steps

1. Load HTML from `case_query.htmlRaw`.
2. Parse the HTML and locate process cards.
3. For each card:

   * Extract identification metadata.
   * Create or update a document in `case_data`.
4. Persist the canonical identifier in `identity.stfDecisionId`.

### 4.4 Output

* Updated or newly created documents in `case_data`.

### 4.5 Status Update

* Update `processing` and `audit`.
* Update `status.pipelineStatus` according to execution outcome.

---

## 5. Pipeline 2 — Obtain Full Process HTML

### 5.1 Objective

Retrieve the full HTML of each STF process decision page.

### 5.2 Input Filter

```
status.pipelineStatus = "caseScraped"
```

### 5.3 Processing Rules

* Documents are processed sequentially (one at a time).
* The process URL is read from:

```
caseIdentification.caseUrl
```

### 5.4 Processing Steps

1. Perform HTTP request to the process URL.
2. Retrieve full page HTML.
3. Persist HTML in:

```
caseContent.caseHtml
```

### 5.5 Status Update

* Update `processing`, `audit`, and `status.pipelineStatus`.

---

## 6. Pipeline 3 — Sanitize Process HTML

### 6.1 Objective

Isolate only the relevant decision content from the full HTML page.

### 6.2 Execution Requirements

* MongoDB connection via `config/mongo.json`.
* User selects:

  * all documents with `status.pipelineStatus = "caseScraped"`, or
  * a single document via `identity.stfDecisionId`.
* Detailed console logging is mandatory.

### 6.3 Extraction Targets

Any of the following selectors may be used:

* XPath:

  ```
  //*[@id="mat-tab-content-0-0"]/div/div
  ```
* Full XPath:

  ```
  /html/body/app-root/app-home/main/app-search-detail/div/div/div[1]/mat-tab-group/div/mat-tab-body[1]/div/div
  ```
* CSS Selector:

  ```
  #mat-tab-content-0-0 > div > div
  ```
* JavaScript Path:

  ```js
  document.querySelector("#mat-tab-content-0-0 > div > div")
  ```

### 6.4 Output

* Persist sanitized HTML in:

```
caseContent.caseHtmlClean
```

### 6.5 Status Rules

* Success: `status.pipelineStatus = "caseHtmlCleaned"`
* Error: update `processing` and `audit` only

---

## 7. Pipeline 4 — Extract Semantic Sections

### 7.1 Objective

Extract structured semantic sections from sanitized HTML content.

### 7.2 Input

```
caseContent.caseHtmlClean
```

### 7.3 Sections to Extract

* Header
* Publicação
* Partes
* Ementa
* Decisão
* Indexação
* Legislação
* Observação
* Doutrina

### 7.4 Structural Pattern

```
div.jud-text > h4 + div
```

### 7.5 Universal XPath Pattern

* Title:

  ```
  //h4[normalize-space(.)='[NAME]']
  ```
* Content:

  ```
  //h4[normalize-space(.)='[NAME]']/following-sibling::div[1]
  ```

### 7.6 Header Identification

* CSS Selector:

  ```
  #mat-tab-content-0-0 > div > div > div:nth-child(1) > div.jud-text
  ```
* XPath:

  ```
  //*[@id="mat-tab-content-0-0"]/div/div/div[1]/div[1]
  ```
* Fallback XPath:

  ```
  //div[@class='jud-text'][.//h4[contains(text(),'ADI')]]
  ```

### 7.7 Output Fields

```
caseContent.raw.header
caseContent.raw.publication
caseContent.raw.parties
caseContent.raw.summary
caseContent.raw.decision
caseContent.raw.keywords
caseContent.raw.legislation
caseContent.raw.notes
caseContent.raw.doctrine
```

---

## 8. Pipeline 5 — Extract Parties and Keywords

## 9. Pipeline 6 - Extract Legislation References

Objetivo: Extrair, normalizar e estruturar referências legislativas mencionadas nos processos jurídicos, transformando texto não estruturado em dados semânticos hierárquicos utilizando inteligência artificial.

### FONTES DE ENTRADA 

- Entrada Primária: caseContent.md.legislation (texto Markdown da seção "Legislação")'
- Formato de Entrada: Texto não estruturado contendo citações legais em português brasileiro


### SAÍDAS

- Saída Principal: caseData.legislationReferences (array estruturado)
- Metadados: Campos de processamento em processing.caseLegislationRefs*
- Status: Atualização de status.pipelineStatus para "legislationExtracted"

Formato de saída:

```json
{
  "caseData": {
    "legislationReferences": [
      {
        "jurisdictionLevel": "federal|state|municipal|unknown",
        "normType": "CF|EC|LC|LEI|DECRETO|RESOLUÇÃO|PORTARIA|OUTRA",
        "normIdentifier": "string",
        "normYear": "ano_com_4_digitos_ou_null",
        "normDescription": "string",
        "normReferences": [
          {
            "articleNumber": "número_inteiro_ou_null",
            "isCaput": "boolean",
            "incisoNumber": "número_inteiro_ou_null",
            "paragraphNumber": "número_inteiro_ou_null",
            "isParagraphSingle": "boolean",
            "letterCode": "letra_minúscula_ou_null"
          }
        ]
      }
    ]
  }
}
```

### PROMPT PARA IA

Extração Legislativa CITO

Extraia referências do texto para o JSON abaixo. Responda APENAS o JSON.

{"caseData":{"legislationReferences":[{"jurisdictionLevel":"federal|state|municipal|unknown","normType":"CF|EC|LC|LEI|DECRETO|RESOLUÇÃO|PORTARIA|OUTRA","normIdentifier":"TIPO-num-ano","normYear":"YYYY","normDescription":"string","normReferences":[{"articleNumber":int|null,"isCaput":bool,"incisoNumber":int|null,"paragraphNumber":int|null,"isParagraphSingle":bool,"letterCode":null}]}]}}
REGRAS

Normalização: Remova zeros à esquerda (ART-00022 -> 22). Formate ID como TIPO-NUM-ANO.

Contexto: Dispositivos na mesma linha ou sequenciais herdam o último articleNumber mencionado.

Flags: CAPUT -> isCaput:true; PAR-ÚNICO -> isParagraphSingle:true.

Jurisdição: Inferir federal (LEG-FED, CF, LC), state (LEG-EST, siglas de estados), ou municipal.

EXEMPLO DE REFERÊNCIA (FEW-SHOT)

Entrada:
LEG-FED CF ANO-1988 ART-00022 INC-00001 ART-00023 INC-00006
LEG-FED LEI-009605 ANO-1998 ART-00025 PAR-00001 PAR-00002
LEG-FED DEC-006514 ANO-2008 ART-00111 PAR-ÚNICO
LEG-EST LEI-001701 ANO-2022 RR

Saída esperada (Resumo):

{
  "caseData": {
    "legislationReferences": [
      {
        "jurisdictionLevel": "federal",
        "normType": "CF",
        "normIdentifier": "CF-1988",
        "normYear": "1988",
        "normDescription": "Constituição Federal",
        "normReferences": [
          {"articleNumber": 22, "isCaput": false, "incisoNumber": 1, "paragraphNumber": null, "isParagraphSingle": false, "letterCode": null},
          {"articleNumber": 23, "isCaput": false, "incisoNumber": 6, "paragraphNumber": null, "isParagraphSingle": false, "letterCode": null}
        ]
      },
      {
        "jurisdictionLevel": "federal",
        "normType": "LEI",
        "normIdentifier": "LEI-9605-1998",
        "normYear": "1998",
        "normDescription": "Lei Ordinária",
        "normReferences": [
          {"articleNumber": 25, "isCaput": false, "incisoNumber": null, "paragraphNumber": 1, "isParagraphSingle": false, "letterCode": null},
          {"articleNumber": 25, "isCaput": false, "incisoNumber": null, "paragraphNumber": 2, "isParagraphSingle": false, "letterCode": null}
        ]
      },
      {
        "jurisdictionLevel": "federal",
        "normType": "DECRETO",
        "normIdentifier": "DEC-6514-2008",
        "normYear": "2008",
        "normDescription": "Decreto",
        "normReferences": [
          {"articleNumber": 111, "isCaput": false, "incisoNumber": null, "paragraphNumber": null, "isParagraphSingle": true, "letterCode": null}
        ]
      },
      {
        "jurisdictionLevel": "state",
        "normType": "LEI",
        "normIdentifier": "LEI-1701-2022",
        "normYear": "2022",
        "normDescription": "Lei Ordinária RR",
        "normReferences": []
      }
    ]
  }
}

TEXTO PARA PROCESSAMENTO:
[INSERIR TEXTO AQUI]

### FLUXO DE PROCESSAMENTO

- Solicita ao usuário o identity.stfDecisionId para processar um único documento.
    Para cada documento selecionado:

    - Se caseContent.md.legislation estiver vazio/blank:
        - Registrar "empty" em processing.caseLegislationRefsStatus
        - Registrar "legislation vazio" em processing.caseLegislationRefsError
    - Atualizar pipeline para "legislationExtracted" (em processing.pipelineStatus e status.pipelineStatus)
    - Não deve chamar a API do modelop de IA.
    - Se caseContent.md.legislation contiver texto:
        - Enviar o texto para a API de IA com o prompt definido.
        - Se a resposta for um JSON válido:
            - Persistir em caseData.legislationReferences
            - Registrar "success" em processing.caseLegislationRefsStatus
            - Atualizar pipeline para "legislationExtracted"
        - Se a resposta não for um JSON válido ou ocorrer erro:
            - Registrar "error" em processing.caseLegislationRefsStatus
            - Registrar mensagem de erro em processing.caseLegislationRefsError
            - Não atualizar status.pipelineStatus

### DADOS API GROQ


GROQ_API_KEY = "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"
GROQ_MODEL = "llama-3.1-8b-instant"
REQUEST_TIMEOUT = int(os.getenv("GROQ_TIMEOUT", "60"))
RETRIES = int(os.getenv("GROQ_RETRIES", "3"))
API_DELAY_SECONDS = 20  # Delay configurável entre processamentos




### 8.1 Execution Rules

* MongoDB connection via `config/mongo.json`.
* User must provide `identity.stfDecisionId`.
* Execution is independent of current pipeline status.

### 8.2 Parties Extraction

#### Input

```
caseContent.md.parties
```

#### Parsing Rule

```
<TYPE>: <NAME>
```

#### Output

```
caseData.caseParties: [
  { "partieType": "...", "partieName": "..." }
]
```

### 8.3 Keywords Extraction

#### Input

```
caseContent.md.keywords
```

#### Parsing Rule

* Split by comma
* Normalize and deduplicate

#### Output

```
caseData.caseKeywords: ["...", "..."]
```

---

## 9. Unified MongoDB Document Schema — case_data

(Full schema preserved exactly as defined in the requirements, including identity, caseIdentification, caseContent, rawData, caseData, processing, status, and sourceIds.)

---

## 10. Final Notes

* `identity.stfDecisionId` is mandatory and canonical.
* `status.pipelineStatus` represents logical workflow state.
* `processing` represents execution metadata.
* `audit` ensures full traceability.

This document is the **authoritative system specification** for the CITO Project and must be kept synchronized with implementation changes.
