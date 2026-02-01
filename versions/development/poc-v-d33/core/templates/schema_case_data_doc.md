# Documentacao da Estrutura de Dados: `case_data`

Esta documentacao descreve o schema da collection `case_data` utilizada no projeto CITO (versao poc-v-d33). O modelo de dados combina validacao via JSON Schema (MongoDB) para campos essenciais e flexibilidade para campos de enriquecimento gerados pelo pipeline de IA.

## 1. Visao Geral da Collection

*   **Collection**: `case_data`
*   **Objetivo**: Armazenar dados de jurisprudencia do STF, desde a coleta (scraping) ate o enriquecimento com IA (extracao de doutrina, legislacao, notas, etc.).
*   **Estrategia**: Schema hibrido. Campos de identificacao e auditoria sao rigidos (validados); campos de conteudo e processamento permitem expansao (`additionalProperties: true`).

## 2. Estrutura do Documento

### 2.1. Raiz
| Campo | Tipo | Descricao |
| :--- | :--- | :--- |
| `_id` | ObjectId | Identificador unico do documento no MongoDB. |
| `identity` | Object | **(Obrigatorio)** Dados de identificacao do processo/decisao. |
| `audit` | Object | **(Obrigatorio)** Metadados de auditoria e controle de versao. |
| `caseContent` | Object | **(Obrigatorio)** Conteudo do processo (HTML, Markdown, Texto). |
| `caseData` | Object | Dados estruturados extraidos/enriquecidos (doutrina, legislacao, notas, etc.). |
| `processing` | Object | Logs e metadados da execucao de scripts/IA. |
| `status` | Object | Controle de estado do pipeline. |
| `caseTitle` | string | Titulo resumido do processo (alias de `identity.caseTitle`). |
| `dates` | Object | Datas normalizadas do processo. |

Observacoes:
- `caseTitle` e `dates` sao redundantes/derivados e podem nao aparecer em todos os documentos.

---

### 2.2. Objeto `identity`
Identifica a decisao judicial.

```json
{
  "stfDecisionId": "string (Obrigatorio)",
  "caseTitle": "string (Obrigatorio)",
  "caseUrl": "string (URL valida)",
  "caseClass": "string",
  "caseNumber": "string",
  "judgingBody": "string",
  "rapporteur": "string",
  "opinionWriter": "string (opcional)",
  "caseQueryId": "string",
  "domClipboardId": "string"
}
```

### 2.3. Objeto `caseContent`
Armazena o teor do documento em diferentes formatos.

```json
{
  "caseUrl": "string (URL valida)",
  "caseHtml": "string (HTML completo/original)",
  "caseHtmlClean": "string (HTML sanitizado)",
  "originalHtml": "string (HTML bruto, alias/legado)",
  "cleanHtml": "string (HTML sanitizado, alias/legado)",
  "md": {
    "header": "string",
    "publication": "string",
    "parties": "string",
    "summary": "string",
    "decision": "string",
    "keywords": "string",
    "legislation": "string",
    "notes": "string",
    "doctrine": "string"
  },
  "raw": {
    "header": "string",
    "publication": "string",
    "parties": "string",
    "summary": "string",
    "decision": "string",
    "keywords": "string",
    "legislation": "string",
    "notes": "string",
    "doctrine": "string"
  }
}
```

Notas:
- `md` e `raw` carregam as secoes extraidas (Markdown/texto bruto), podendo faltar algum bloco dependendo do documento fonte.

### 2.4. Objeto `caseData`
Armazena o resultado do enriquecimento de dados.

#### 2.4.1. `caseKeywords` (Array de strings)
Lista de palavras-chave extraidas.

#### 2.4.2. `caseParties` (Array de objetos)
```json
{
  "partieType": "string",
  "partieName": "string"
}
```

#### 2.4.3. `legislationReferences` (Array de objetos)
```json
{
  "normIdentifier": "string",
  "jurisdictionLevel": "string",
  "normType": "string",
  "normYear": "int",
  "normDescription": "string",
  "normReferences": [
    {
      "articleNumber": "int",
      "isCaput": "bool",
      "incisoNumber": "int | null",
      "paragraphNumber": "int | null",
      "isParagraphSingle": "bool",
      "letterCode": "string | null"
    }
  ]
}
```

#### 2.4.4. `notesReferences` (Array de objetos)
```json
{
  "noteType": "string",
  "rawLine": "string",
  "items": [
    {
      "itemType": "string",
      "caseClass": "string | null",
      "caseNumber": "string | null",
      "suffix": "string | null",
      "orgTag": "string | null",
      "country": "string | null",
      "rawRef": "string"
    }
  ]
}
```

#### 2.4.5. `doctrineReferences` (Array de objetos)
Lista de citacoes doutrinarias extraidas.

```json
{
  "author": "string",
  "publicationTitle": "string",
  "edition": "string | null",
  "publicationPlace": "string | null",
  "publisher": "string | null",
  "year": "int | null",
  "page": "string | null",
  "rawCitation": "string"
}
```

#### 2.4.6. `decisionDetails` (Objeto)
Detalhes estruturados da decisao (quando a etapa correspondente e executada).

```json
{
  "decisionSummary": {
    "summary": "string"
  },
  "decisionResult": {
    "finalDecision": "string"
  },
  "ministerVotes": [
    {
      "ministerName": "string | null",
      "voteType": "string",
      "voteSummary": "string | null"
    }
  ],
  "partyRequests": [
    {
      "requestType": "string",
      "requestingParty": "string | null",
      "requestDescription": "string",
      "requestOutcome": "string"
    }
  ],
  "speakers": [
    {
      "lawyerName": "string",
      "representedParty": "string",
      "argumentSummary": "string | null"
    }
  ],
  "legalBasis": {
    "summary": "string"
  },
  "citations": [
    {
      "citationType": "string",
      "citationName": "string",
      "relevanceSummary": "string"
    }
  ],
  "decisionType": "string",
  "decisionDates": {
    "judgmentStartDate": "string (YYYY-MM-DD)",
    "judgmentEndDate": "string (YYYY-MM-DD)"
  }
}
```

### 2.5. Objeto `processing`
Rastreabilidade da execucao de tarefas (ex: scraping, extracao e chamadas de IA).

Campos observados:
- `pipelineStatus`
- `caseScrapeStatus`, `caseScrapeError`, `caseScrapeAt`, `caseScrapeHttpStatus`, `caseScrapeLatencyMs`, `caseScrapeHtmlBytes`
- `caseHtmlCleaningAt`, `caseHtmlCleanedAt`, `caseHtmlCleanError`, `caseHtmlCleanMeta`
- `caseSectionsExtractingAt`, `caseSectionsExtractedAt`, `caseSectionsError`, `caseSectionsMeta`
- `partiesKeywords.finishedAt`, `partiesKeywords.partiesCount`, `partiesKeywords.keywordsCount`
- `caseLegislationRefsStatus`, `caseLegislationRefsError`, `caseLegislationRefsAt`, `caseLegislationRefsProvider`, `caseLegislationRefsModel`, `caseLegislationRefsLatencyMs`
- `caseNotesRefsStatus`, `caseNotesRefsError`, `caseNotesRefsAt`, `caseNotesRefsProvider`, `caseNotesRefsModel`, `caseNotesRefsLatencyMs`
- `caseDoctrineStatus`, `caseDoctrineError`, `caseDoctrineAt`, `caseDoctrineProvider`, `caseDoctrineModel`, `caseDoctrineLatencyMs`, `caseDoctrineCount`
- `caseDecisionDetailsStatus`, `caseDecisionDetailsError`, `caseDecisionDetailsAt`, `caseDecisionDetailsProvider`, `caseDecisionDetailsModel`, `caseDecisionDetailsLatencyMs`

### 2.6. Objeto `audit`
Controle temporal e de status macro.

```json
{
  "extractionDate": "Date",
  "lastExtractedAt": "Date",
  "builtAt": "Date",
  "updatedAt": "Date",
  "sourceStatus": "string",
  "pipelineStatus": "string",
  "lastCaseHtmlCleanedAt": "Date",
  "lastSectionsExtractedAt": "Date"
}
```

### 2.7. Objeto `status`
Controle de fluxo do pipeline principal.

```json
{
  "pipelineStatus": "string (ex: doctrineExtracted, decisionDetailsExtracted)",
  "updatedAt": "Date"
}
```

### 2.8. Objeto `dates`
Datas normalizadas do processo (quando presente).

```json
{
  "judgmentDate": "string (DD/MM/YYYY)",
  "publicationDate": "string (DD/MM/YYYY)"
}
```

Observacao: nos documentos atuais as datas aparecem como `string` no formato `DD/MM/YYYY`. Em migracoes/analiticos pode haver normalizacao para `Date`.

 
