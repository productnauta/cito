Com base no script em contexto, quero criar um novo script py, utilizando os mesmo padrões de conexão, configuração, schema, dados e código utilizados nos scripts [step07-extract-notes-mistral.py](versions/development/poc-v-d33/core/step07-extract-notes-mistral.py) [step08-doctrine-mistral.py](versions/development/poc-v-d33/core/step08-doctrine-mistral.py) [step06-extract-legislation-mistral.py](versions/development/poc-v-d33/core/step06-extract-legislation-mistral.py) (uso do módulo de conexão com o banco, uso dos arquivos yaml de configs, logs, persistencias, etc...)


Atualmente a URL é montada pela função build_target_url() usando urlunparse com:

Base:
    scheme: DEFAULT_URL_SCHEME (ex.: https)
    netloc: DEFAULT_URL_NETLOC (ex.: jurisprudencia.stf.jus.br)
    path: DEFAULT_URL_PATH (ex.: /pages/search)
    Parâmetros usados (query string)
Dinâmicos:
    pesquisa_inteiro_teor (true/false)
    pageSize (tamanho da página)
    queryString (termo de busca)
Fixos (de FIXED_QUERY_PARAMS):
    base=acordaos
    sinonimo=true
    plural=true
    radicais=false
    buscaExata=true
    page=1
    sort=_score
    sortBy=desc
    processo_classe_processual_unificada_classe_sigla (repetido para cada classe: ADC, ADI, ADO, ADPF)

Exemplo de estrutura final:
    https://jurisprudencia.stf.jus.br/pages/search?pesquisa_inteiro_teor=...&pageSize=...&queryString=...&base=acordaos&...&processo_classe_processual_unificada_classe_sigla=ADC&processo_classe_processual_unificada_classe_sigla=ADI&...

Todas essas configurações (parâmetros dinâmicos e fixos) devem ser lidas do arquivo query.yaml em config, 
    - Reestruture o arquivo query.yaml, organizando os parâmetros em seções lógicas (query, http, runtime, url, fixed_query_params).
    - Utilize nomenclaturas em inglês para as chaves do YAML.
    - Remova chaves duplicadas.

### ETAPA 1: PESQUISAR STF E IDENTIFICAR PROCESSOS

1. Obter as informçaões e parametros de busca disponíveis no arquivo de configuração YAML query.yaml `versions/development/poc-v-`33/config`.
2.  Montar a URL de busca com base nos parâmetros fornecidos.
3.  Utiliza playwright para realizar a busca no site do STF.
    1.  Captura todo o HTML via page.content()
4.  Inserir o HTML bruto da página de resultados na collection `case_query` do MongoDB, com status `"new"`.

