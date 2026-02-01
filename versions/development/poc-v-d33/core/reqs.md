


Crie um script em python, com os seguintes requisitos e características:

## DEFINIÇÕES GLOBAIS

    - path da localização do sript: versions/development/poc-v-d33/core
    - path dos arquivos yaml de configuração: versions/development/poc-v-d33/config
    - arquivos de configuração:
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
    - Utilizar os padrões de dados e arquiteturais da versão "poc-v-d33"   
        - path da verão:: versions/development/poc-v-d33/
    - Schema atual JSON do documento da collection "case_data"
        - path do schema: versions/development/poc-v-d33/core/templates/schema_case_data.json
    - Persisência de lgos e status: o manual no path: versions/development/poc-v-d33/core/templates/persistencia-status.md
    - Ao iniciar a execussão do script, obter dados, salvar dados, manipular ou processar informações ou etapas do código, exiba no terminal mensagens de log detalhadas sobre, inclua data e hora no início da mensagem no formato: yy-mm-dd hh:mm:ss. Ex.: [310125 21:33:10] - INICIANDO EXECUSSÃO DO SCRIPT
GERA
    - Incluir comentários explicativos à cada bloco de código, ou trecho complexo.

    - PERSISTÊNCIA DE STATUS


que solciite ao usuário um stfDecisionId, em seguida busque o campo `case_data.caseContent.md.legislation` da decisão correspondente ao stfDecisionId informado e imprima o resultado na tela.

Caso não exista decisão correspondente ao stfDecisionId informado, o script deve informar ao usuário que a decisão não foi encontrada, e concluir a execução

Caso exista o dado `caseContent.md,legislation`, 


Obtenha os dados de conexão com o banco de dados MongoDB no arquivo de configuração (/workspaces/cito/versions/poc-v-d33/config/mongo.json)

Utilizando a api para python Mistral, 