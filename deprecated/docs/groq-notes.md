
Crie um novo script pytho, utilizando os dados de conexão com o mongo db do arquivo `config/mongo.json`, e com os dados da api groq do arquivo `config/ai-model.json`.

Solicite ao usuário o identity.stfDecisionId para processar um único documento.


Para cada documento selecionado

Obtenha o conteúdo do campo caseContent.md.notes.
Envie o conteúdo para a api groq, utilizando os parametros do arquivo `config/ai-model.json`.

O Objetivo é extrair citações de acórdãos, decisões monocráticas, decisões de outros tribunais, legislações estrangeiras, decisões estrangeiras e referências "veja".

from groq import Groq

client = Groq()
completion = client.chat.completions.create(
    model="llama-3.1-8b-instant",
    messages=[
      {
        "role": "system",
        "content": "# ROLE\nVocê é um extrator de dados jurídicos especializado em tribunais superiores e direito comparado. Sua tarefa é converter citações textuais em um JSON estruturado e rigoroso.\n\n# OUTPUT RULES\n- Retorne APENAS o objeto JSON.\n- Não inclua explicações ou blocos de Markdown fora do JSON.\n- Se um campo for inexistente, use `null`.\n- Mantenha a grafia original em `rawLine` e `rawRef`.\n\n# EXTRACTION LOGIC & MAPPING\n1. **Categorização (noteType):**\n   - \"Acórdãos citados\" -> `stf_acordao`\n   - \"Decisão monocrática\" -> `stf_monocratica`\n   - \"Decisões de outros tribunais\" -> `outros_tribunais`\n   - \"Legislação estrangeira\" -> `legislacao_estrangeira`\n   - \"Decisões estrangeiras\" -> `decisao_estrangeira`\n   - Referências iniciadas por \"Veja\" ou \"Cf.\" -> `veja`\n\n2. **Parsing de Itens (Brasil):**\n   - **caseClass:** Sigla da classe (ex: ADI, RE, HC, ADPF).\n   - **caseNumber:** Apenas os dígitos numéricos.\n   - **suffix:** Siglas acessórias (ex: MC, QO, AgR, ED, RG).\n   - **orgTag:** Órgão julgador, se houver (ex: TP, 1ªT).\n\n3. **Tratamento Especial:**\n   - Documentos da ONU, Tratados, Planos ou Recomendações do CNJ -> `itemType: \"treaty_or_recommendation\"`.\n   - Citações de Revistas (ex: RTJ 63/299) -> `itemType: \"legal_journal\"`.\n\n# JSON SCHEMA\n{\n  \"caseData\": {\n    \"notesReferences\": [\n      {\n        \"noteType\": \"string\",\n        \"rawLine\": \"string\",\n        \"items\": [\n          {\n            \"itemType\": \"decision | legislation | treaty_or_recommendation | legal_journal\",\n            \"caseClass\": \"string | null\",\n            \"caseNumber\": \"string | null\",\n            \"suffix\": \"string | null\",\n            \"orgTag\": \"string | null\",\n            \"country\": \"string | null\",\n            \"rawRef\": \"string\"\n          }\n        ]\n      }\n    ]\n  }\n}"
      },
      {
        "role": "user",
        "content": "Extraia as citações do texto abaixo seguindo o SCHEMA definido. \nAtenção: Separe cada citação individualmente (ex: \"ADPF 54 MC\" e \"ADPF 54 QO\" são dois itens distintos).\n\nTEXTO PARA ANÁLISE:\n[COLE O TEXTO AQUI]"
      }
    ],
    temperature=0,
    max_completion_tokens=8000,
    top_p=1,
    stream=False,
    stop=None
)

print(completion.choices[0].message)

Salve a resposta em caseData.notesReferences.
Atualize status audit e processing
imprima no terminal o status da operação.
Conte e impra o total de itens identificados e extraidos
Comente o código