****


Crie um script em python, com os seguintes requisitos e características:

## **DEFINIÇÕES GLOBAIS**

* **Path da da versão**: versions/development/poc-v-d33/core
* **Path dos arquivos configuração**: versions/development/poc-v-d33/config
* **Arquivos de configuração**:
    - mongo.yaml: dados de conexão com o banco de dados
        - mongo:
            - user: nome usuário de conexão 
            - password: senjha de conexão
            - uri: string uri de conexão
            - database: bome da collextion
    - providers.yaml: dados de conexão e configuração das apis de IA como Mistral, Gemini, Groq, etc.
            - providers: lista de provedores de modelos de IA
            - name: nome do provedor de IA
            - defaults: configurações padrão aplicadas ao provedor
                - model: modelo de IA utilizado por padrão
                - temperature: controle de aleatoriedade das respostas
                - max_tokens: limite máximo de tokens gerados na resposta
                - top_p: parâmetro de amostragem probabilística (nucleus sampling)
                - request_timeout_seconds: tempo máximo de espera por resposta da API
                - retries: número de tentativas em caso de falha
                - api_delay_seconds: intervalo de espera entre requisições
            - keys: lista de chaves de acesso ao provedor
                - name: identificador da chave
                - key: variável que contém a chave de API
    - prompts.yaml: prompts e parametrizações dos prompts
            - prompts: mapeamento de prompts por identificador
               - <prompt_id>: objeto que define um prompt específico
                        - template: lista ordenada de mensagens que compõem o prompt
                           - role: papel da mensagem no diálogo (system, user ou assistant)
                           - content: texto ou template multilinha com instruções ou solicitação
                        - template_variables: lista de variáveis/placeholders usados no content
                        - metadata: metadados descritivos e de controle do prompt
                            - name: nome legível do prompt
                            - description: propósito resumido e observações de uso
                            - tags: palavras-chave para categorização e busca
                            - version: versão semântica do prompt
                            - author: autor ou responsável pelo prompt
                        - client_parameters: parâmetros enviados ao cliente/modelo de LLM
                            - temperature: grau de aleatoriedade da resposta
                            - max_tokens: limite máximo de tokens de saída
                            - top_p: parâmetro de amostragem probabilística
                            - outros: parâmetros adicionais do cliente (timeout, retries, etc.)
                        - defaults: preferências de provedor, modelo ou configurações padrão
                        - schema: definição estrutural dos campos esperados na saída
                        - normalization: regras de normalização aplicadas aos dados extraídos
                        - output: especificação do formato de saída gerado pelo prompt
                        - examples: exemplos de entrada e saída para orientar o agente
                        - tests: casos de teste automatizados com entrada e saída esperada
                        - metadata_extra: metadados auxiliares como parser recomendado ou script de validação
* **Padrões do projeto**: Utilizar os padrões de dados e arquiteturais da versão "poc-v-d33"   
* **Documento JSON case_data**: Padrões, schema, estrutura e documentação da collection case_data: 
        - Schema JSON do documento: versions/development/poc-v-d33/core/templates/schema_case_data.json
        - Documentação do schema: versions/development/poc-v-d33/core/templates/schema_case_data_doc.md
* **Persistência de status e execussões**:
        - manual no path: versions/development/poc-v-d33/core/templates/persistencia-status.md
* **Exibição de logs detalhados no terminal**:
        - Ao iniciar a execussão do script, obter dados, salvar dados, manipular ou processar informações ou etapas do código, exiba no terminal mensagens de log detalhadada.
        - inclua data e hora no início da mensagem 
            - formato: yy-mm-dd hh:mm:ss. 
            - Ex.: [310125 21:33:10] - INICIANDO EXECUSSÃO DO SCRIPT 
* **Comentários no código**: 
        - Incluir comentários explicativos à cada bloco de código, ou trecho complexo.
        - Incluir comentários de cabeçalho no início do script com informações sobre o propósito do script, autor, data de criação e versão.

## REQUISITOS DO SCRIPT

**Descrição**: O script deve iniciar carregando a partir dos arquivos YAML de **configuração**, os dados necessárias para conectar ao banco de dados MongoDB e para utilizar a API Mistral, e os prompts e parametros que serão utilziados.

    * Utilziar variáveis e apropriadas para amarmazenar os dados de configuração e parametrização para uso posterior no script.
    * Estruturar o código, para que seja facilmente definido qual provider de IA e prompt serão utilizados na etapa de consulta à API Mistral.
        - Deve ser possível definir o provider de IA e prompt a serem utilizados, alterando apenas uma variável no início do script, e as demais informações, parametros e configurações serão carregadas do respectivo itens dos arquivos de configuração.  
            - Exemplo: definindo no script o provider_ia = "mistral", o sistema deverá carregar as respectivas configurações do provider Mistral do arquivo providers.yaml: model, key, temperature, max_tokens, etc (apenas as que serão utilizadas).
            - Exemplo: definindo no script o prompt_id = "extract-notes-from-md", o sistema deverá carregar as respectivas configurações do prompt que serão utilizadas (apenas as que serão utilizadas) do arquivo prompts.yaml: role, content, template_variables, client_parameters, etc.   
    * MongoDB: Obter eos dados de conexão com o mongo DB e incluir em variáveis para realizar a conexão com o banco de dados.


## PIPELINE DE EXECUSSÃO DO SCRIPT

1. Solicitar ao usuário a entrada do stfDecisionId via input no terminal.
   1. Deve ser possível utilizar o script externament, enviando o stfDecisionId como parâmetro na linha de comando. 
      - Exemplo: python script.py --stfDecisionId <valor_stfDecisionId>
2. Consultar a collection case_data no banco de dados MongoDB, buscando o documento onde o campo `stfDecisionId` seja igual ao valor informado pelo usuário.
    1. Se o documento não for encontrado, informar ao usuário que a decisão não foi encontrada.
       1. Concluir a execução do script.
    2. Se o documento for encontrado, verificar se o campo `case_data.caseContent.md.notes` existe.
      1. Se o campo existir, imprimir o conteúdo do campo `case_data.caseContent.md.notes` na tela.
      2. Se o campo não existir, informar ao usuário que o campo `case_data.caseContent.md.notes` não foi encontrado na decisão.
         1. Concluir a execução do script.
3. Obter o valor do campo `case_data.caseContent.md,notes` do documento encontrado.
4. Utilizando a API Mistral, enviar uma requisição para a API Mistral, utilizando os dadoos e parametros do prompt definido no início do script.
   1. Prompt definido: "extract-notes-from-md"
   2. Utilizar os roles e contents definidos no prompt.
   3. Utilizar os parâmetros de client_parameters definidos no prompt.
5. Obter a resposta da API Mistral.
   1. Imprimir a resposta da API Mistral na tela.
6. Identificar dados e montar documento JSON com as informações identificadas na resposta da API Mistral.
7. Lógica e regras para identificar e montar o documento JSON devem ser definidas conforme o schema do documento case_data.



## Definição das Tags e Campos

### 1. **H - Header (Cabeçalho/Categoria)**
Define o tipo de bloco de citação que está sendo processado. Todos os itens subsequentes pertencem a esta categoria até que um novo `H` apareça.

**Formato:**
```
H|<CODIGO_TIPO>
```

**Códigos Válidos:**
| Código | Descrição                     |
|--------|-------------------------------|
| AC     | Acórdão STF                   |
| MO     | Decisão Monocrática STF       |
| LE     | Legislação Estrangeira        |
| DE     | Decisão Estrangeira           |
| VJ     | Veja / Outros                 |

---

### 2. **D - Descriptors (Descritores)**
Captura as palavras-chave de contexto (geralmente encontradas entre parênteses e em caixa alta antes das citações).

**Formato:**
```
D|<DESCRITORES>
```

**Regra:**
Múltiplos descritores são separados por ponto e vírgula `;`.

**Exemplo:**
```
D|COMPETÊNCIA;UNIÃO FEDERAL
```

---

### 3. **L - Line (Linha Original)**
Registra a linha de texto original do documento que está sendo analisada. Serve como referência para os itens extraídos.

**Formato:**
```
L|<LINHA_ORIGINAL>
```

**Exemplo:**
```
L|ADI 1234 (TP), RE 5678 (TP).
```

---

### 4. **I - Item (Item Extraído)**
Representa uma citação individual extraída da linha `L` imediatamente anterior.

**Formato:**
```
I|<TIPO_ITEM>|<CLASSE>|<NUMERO>|<SUFIXO>|<ORGAO>|<REF_BRUTA>
```

**Campos:**
| Campo      | Descrição                                                                 |
|------------|---------------------------------------------------------------------------|
| TIPO_ITEM  | `S`: Para casos do STF (Estruturados); `O`: Para outros tipos (Texto livre). |
| CLASSE     | (Apenas STF) Ex: ADI, RE, HC.                                             |
| NUMERO     | (Apenas STF) Ex: 1234.                                                    |
| SUFIXO     | (Apenas STF) Ex: AgR, MC, MC-Ref.                                         |
| ORGAO      | (Apenas STF) Ex: TP, 1ªT, 2ªT.                                            |
| REF_BRUTA  | O texto exato da citação.                                                 |

**Regra para "Outros" (O):**
Os campos de classe, número, sufixo e órgão ficam vazios (ex: `||||`).

---

### 5. **M - Metadata (Metadados)**
Captura as informações de rodapé da análise.

**Formato:**
```
M|<PAGINAS>|<DATA_ISO>|<ANALISTA>
```

**Campos:**
| Campo     | Descrição                                      |
|-----------|------------------------------------------------|
| PAGINAS   | Número inteiro de páginas.                     |
| DATA_ISO  | Data da análise no formato `YYYY-MM-DD`.       |
| ANALISTA  | Iniciais do responsável pela análise.          |

---

## Exemplos

### Texto de Entrada:
```
- Acórdão(s):
(TEMA A)
ADI 1234 (TP).
Páginas: 1. Análise: 01/01/2024, ABC.
```

### Saída no Protocolo:
```
H|AC
D|TEMA A
L|ADI 1234 (TP).
I|S|ADI|1234||TP|ADI 1234 (TP)
M|1|2024-01-01|ABC
```

8. 
9. Formato de saída esperado
# JSON SCHEMA
{
  "caseData": {
    "notesReferences": [
      {
        "noteType": "string",
        "rawLine": "string",
        "items": [
          {
            "itemType": "decision | legislation | treaty_or_recommendation | legal_journal",
            "caseClass": "string | null",
            "caseNumber": "string | null",
            "suffix": "string | null",
            "orgTag": "string | null",
            "country": "string | null",
            "rawRef": "string"
          }
        ]
      }
    ]
  }
}

1. Salvar o documento JSON montado no campo `case_data.notesReferences` do documento correspondente ao stfDecisionId informado.
2.  Persistir logs e status da execução do script conforme o manual de persistência de status e execussões. 
    
