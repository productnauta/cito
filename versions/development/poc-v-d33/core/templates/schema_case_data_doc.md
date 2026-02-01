# Documentação da Estrutura de Dados: `case_data`

Esta documentação descreve o schema da collection `case_data` utilizada no projeto CITO (versão poc-v-d33). O modelo de dados combina validação via JSON Schema (MongoDB) para campos essenciais e flexibilidade para campos de enriquecimento gerados pelo pipeline de IA.

## 1. Visão Geral da Collection

*   **Collection**: `case_data`
*   **Objetivo**: Armazenar dados de jurisprudência do STF, desde a coleta (scraping) até o enriquecimento com IA (extração de doutrinas, legislação, etc.).
*   **Estratégia**: Schema híbrido. Campos de identificação e auditoria são rígidos (validados); campos de conteúdo e processamento permitem expansão (`additionalProperties: true`).

## 2. Estrutura do Documento

### 2.1. Raiz
| Campo | Tipo | Descrição |
| :--- | :--- | :--- |
| `_id` | ObjectId | Identificador único do documento no MongoDB. |
| `identity` | Object | **(Obrigatório)** Dados de identificação do processo/decisão. |
| `audit` | Object | **(Obrigatório)** Metadados de auditoria e controle de versão. |
| `caseContent` | Object | **(Obrigatório)** Conteúdo do processo (HTML, Markdown, Texto). |
| `caseData` | Object | Dados estruturados extraídos/enriquecidos (ex: Doutrinas). |
| `processing` | Object | Logs e metadados da execução de scripts/IA. |
| `status` | Object | Controle de estado do pipeline. |

---

### 2.2. Objeto `identity`
Identifica a decisão judicial.

```json
{
  "stfDecisionId": "string (Obrigatório)",
  "caseTitle": "string (Obrigatório)",
  "caseUrl": "string (URL válida)",
  "caseClass": "string",
  "caseNumber": "string",
  "judgingBody": "string",
  "rapporteur": "string",
  "judgmentDate": "string (DD/MM/YYYY)",
  "publicationDate": "string (DD/MM/YYYY)",
  "caseCode": "string",
  "caseClassDetail": "string",
  "caseNumberDetail": "string",
  "caseQueryId": "string",
  "domResultContainerId": "string"
}
```

### 2.3. Objeto `caseContent`
Armazena o teor do documento em diferentes formatos.

```json
{
  "caseUrl": "string (URL válida)",
  "caseHtml": "string (HTML completo/original)",
  "originalHtml": "string (HTML bruto, alias/legado)",
  "cleanHtml": "string (HTML sanitizado)",
  "md": {
    "doctrine": "string (Texto da seção de Doutrina extraído para processamento)",
    "legislation": "string (Texto da seção de Legislação)",
    "notes": "string (Texto da seção de Observações)"
  }
}
```
*Nota: O campo `md` é utilizado pelos scripts de extração (ex: `step08-doctrine-legislation-ai.py`) para alimentar os modelos de IA.*

### 2.4. Objeto `caseData`
Armazena o resultado do enriquecimento de dados.

#### `caseDoctrines` (Array de Objetos)
Lista de citações doutrinárias extraídas via IA.

```json
[
  {
    "author": "string (Nome do autor)",
    "publicationTitle": "string (Título da obra/artigo)",
    "edition": "string | null (Edição normalizada)",
    "publicationPlace": "string | null (Local)",
    "publisher": "string | null (Editora)",
    "year": "int | null (Ano de publicação, 4 dígitos)",
    "page": "string | null (Páginas citadas)",
    "rawCitation": "string (Texto original da citação)"
  }
]
```

### 2.5. Objeto `processing`
Rastreabilidade da execução de tarefas (ex: chamadas de IA).

```json
{
  "caseDoctrineStatus": "success | error",
  "caseDoctrineError": "string | null",
  "caseDoctrineAt": "Date (UTC)",
  "caseDoctrineProvider": "string (ex: groq)",
  "caseDoctrineModel": "string (ex: llama-3.1-8b-instant)",
  "caseDoctrineLatencyMs": "int",
  "caseDoctrineCount": "int",
  "pipelineStatus": "string (Status detalhado da etapa)"
}
```

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
  "pipelineStatus": "string (ex: doctrineExtracted, caseScraped)"
}
```