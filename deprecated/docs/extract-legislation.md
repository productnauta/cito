## 1. Extract Legislation References

### 1.1 General Requirements
- Use MongoDB connection parameters from `config/mongo.json`.
- Emit detailed and structured logs for every execution step.
- Ensure idempotent and atomic updates to the document.

---

### 1.2 Objective
Extract, normalize, and structure legislative references cited in legal decisions, converting unstructured Brazilian Portuguese text into hierarchical semantic data using an AI model.

---

### 1.3 Input
- **Source Field**: `caseContent.md.legislation`
- **Format**: Unstructured text (PT-BR) with legal citations

---

### 1.4 Outputs
- **Primary Output**: `caseData.legislationReferences`
- **Processing Metadata**: `processing.caseLegislationRefs*`
- **Pipeline Status (success or empty input)**:
```

status.pipelineStatus = "legislationExtracted"

````

---

### 1.5 Output Schema
```json
{
"caseData": {
  "legislationReferences": [
    {
      "jurisdictionLevel": "federal|state|municipal|unknown",
      "normType": "CF|EC|LC|LEI|DECRETO|RESOLUÇÃO|PORTARIA|OUTRA",
      "normIdentifier": "string",
      "normYear": "YYYY|null",
      "normDescription": "string",
      "normReferences": [
        {
          "articleNumber": "int|null",
          "isCaput": "boolean",
          "incisoNumber": "int|null",
          "paragraphNumber": "int|null",
          "isParagraphSingle": "boolean",
          "letterCode": "string|null"
        }
      ]
    }
  ]
}
}
````

---

### 1.6 AI Prompt Specification

* **Prompt Name**: Extração Legislativa CITO
* **Instruction**: Extract legislative references from the provided text and return **only** a JSON strictly matching the defined schema.

```json
{"caseData":{"legislationReferences":[{"jurisdictionLevel":"federal|state|municipal|unknown","normType":"CF|EC|LC|LEI|DECRETO|RESOLUÇÃO|PORTARIA|OUTRA","normIdentifier":"TIPO-NUM-ANO","normYear":"YYYY|null","normDescription":"string","normReferences":[{"articleNumber":int|null,"isCaput":bool,"incisoNumber":int|null,"paragraphNumber":int|null,"isParagraphSingle":bool,"letterCode":null}]}]}}
```

#### 1.6.1 Normalization Rules

* Remove leading zeros (e.g., `ART-00022` → `22`).
* Normalize identifiers to `TIPO-NUM-ANO`.
* Devices on the same line or sequential lines inherit the last declared `articleNumber`.
* Flags:

  * `CAPUT` → `isCaput = true`
  * `PAR-ÚNICO` → `isParagraphSingle = true`
* Jurisdiction inference:

  * **Federal**: `CF`, `LC`, `LEG-FED`
  * **State**: `LEG-EST`, state acronyms
  * **Municipal**: explicit municipal indicators
* Missing data must be set to `null`. No inference beyond explicit text.

---

### 1.7 Few-Shot Example

**Input**

```
LEG-FED CF ANO-1988 ART-00022 INC-00001 ART-00023 INC-00006
LEG-FED LEI-009605 ANO-1998 ART-00025 PAR-00001 PAR-00002
LEG-FED DEC-006514 ANO-2008 ART-00111 PAR-ÚNICO
LEG-EST LEI-001701 ANO-2022 RR
```

**Expected Output (Excerpt)**

```json
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
          { "articleNumber": 22, "isCaput": false, "incisoNumber": 1, "paragraphNumber": null, "isParagraphSingle": false, "letterCode": null },
          { "articleNumber": 23, "isCaput": false, "incisoNumber": 6, "paragraphNumber": null, "isParagraphSingle": false, "letterCode": null }
        ]
      }
    ]
  }
}
```

---

### 1.8 Processing Flow

1. Request `identity.stfDecisionId`.
2. Load the corresponding document from `case_data`.
3. If `caseContent.md.legislation` is empty or blank:

   * `processing.caseLegislationRefsStatus = "empty"`
   * `processing.caseLegislationRefsError = "legislation vazio"`
   * Update `status.pipelineStatus = "legislationExtracted"`
   * Do **not** invoke the AI API.
4. If content exists:

   * Invoke the AI API using the defined prompt.
   * If a valid JSON is returned:

     * Persist to `caseData.legislationReferences`.
     * `processing.caseLegislationRefsStatus = "success"`
     * Update `status.pipelineStatus = "legislationExtracted"`.
   * If the response is invalid or an error occurs:

     * `processing.caseLegislationRefsStatus = "error"`
     * Populate `processing.caseLegislationRefsError`.
     * Do **not** update `status.pipelineStatus`.

---

### 1.9 AI Model Configuration

* Load AI provider parameters from `config/ai-models.json`.
* The Python script must explicitly select the provider and load its configuration dynamically.

**Example**

```python
ai_model_config = load_ai_model_config("groq")  # or "mistral", "google_gemini"
```

```
```


