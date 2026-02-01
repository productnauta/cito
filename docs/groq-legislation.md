


Crie um novo script pytho, utilizando os dados de conexão com o mongo db do arquivo `config/mongo.json`, e com os dados da api groq do arquivo `config/ai-model.json`.

Solicite ao usuário o identity.stfDecisionId para processar um único documento.


Para cada documento selecionado

Obtenha o conteúdo do campo caseContent.md.legislation.
Envie o conteúdo para a api groq, utilizando os parametros do arquivo `config/ai-model.json`, e o prompt otimizado abaixo.

Utilize o exemplo abaixo para estruturar a requisição para api groq.

client = Groq()
completion = client.chat.completions.create(
    model="llama-3.1-8b-instant",
    messages=[
      {
        "role": "system",
        "content": "Extraia referências legislativas de textos jurídicos brasileiros para o JSON especificado.\n\n### REGRAS DE NORMALIZAÇÃO:\n1. **Identificação**: Agrupar referências pela mesma norma (`normIdentifier`).\n2. **normIdentifier**: Padrão `TIPO-NUMERO-ANO` (Ex: LEI-8112-1990). Remova pontos de milhares.\n3. **normType**: CF, EC, LC, LEI, DECRETO, RESOLUCAO, PORTARIA, OUTRA.\n4. **jurisdictionLevel**: federal, state, municipal, unknown.\n5. **articleNumber**: Apenas o número inteiro.\n6. **isCaput**: `true` se houver a palavra \"caput\" OU se apenas o artigo for citado (sem incisos/parágrafos).\n7. **incisoNumber**: Converter Romano para Inteiro (Ex: \"IV\" -> 4).\n8. **paragraphNumber**: Inteiro. Se \"parágrafo único\", `isParagraphSingle: true`.\n9. **letterCode**: Apenas a letra da alínea (Ex: \"a\").\n\n### FORMATO DE SAÍDA (JSON APENAS):\n{\n  \"caseData\": {\n    \"legislationReferences\": [\n      {\n        \"normIdentifier\": \"string\",\n        \"jurisdictionLevel\": \"string\",\n        \"normType\": \"string\",\n        \"normYear\": 0,\n        \"normDescription\": \"string\",\n        \"normReferences\": [\n          {\n            \"articleNumber\": 0,\n            \"isCaput\": boolean,\n            \"incisoNumber\": null|int,\n            \"paragraphNumber\": null|int,\n            \"isParagraphSingle\": boolean,\n            \"letterCode\": null|string\n          }\n        ]\n      }\n    ]\n  }\n}"
      },
      {
        "role": "user",
        "content": "Extraia as referências do texto abaixo no formato JSON definido. Não inclua explicações ou texto introdutório.\n\nTEXTO:\n{texto_entrada}"
      }
    ],
    temperature=0,
    max_completion_tokens=8000,
    top_p=1,
    stream=False,
    stop=None
)

print(completion.choices[0].message)


