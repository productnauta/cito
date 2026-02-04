
Alterações na interface "Doutrinas" 

O nome da interface deverá ser alterada para "Análise de Citações", e passará a agrupar informações referentes não apenas à citações doutrinárias, mas também informações sobre citações legislativas, jurisprudenciais, acordãos, etc.


Remover os seguintes cards:
    - Casos por Ano (media)
    - Total de Casos
    - Taxa de Votos Vencidos
    - Relacao Doutrina vs Legislacao
    - Distribuicao de Decisoes
    - Ministros Mais Ativos
    - Casos por Ministro
    - Distribuicao de Decisoes
    - Evolucao Temporal de Casos
    - Doutrina vs Legislacao
    - Relatores


## NOVA COMPOSIÇÃO ESPERADA DA INTERFACE




## 1. Bloco de Filtros

* Filtros por:

  * Autor
  * Título da publicação
  * Classe processual
  * Ano de julgamento
  * Relator

---

## 2. BIG NUMBERS (INDICADORES PRINCIPAIS)

### LINHA 1

Em uma linha, exibir os 4 cards descritos abaixo, logo após o card com as opções de filtro e busca.

1. **Autores** - Total de autores citados em todos os processos
2. **Obras** - Total de obras citadas em todos os processos
3. **Legislações** - Total de legislações (únicas) citadas em todos os processos
4. **Acordãos** - Total de acordãos únicos citados em todos os processos


### LINHA 2
Em uma linha, exibir os cards descritos abaixo, dividindo o espaço segundo à proporção definida:

1. **Doutrinas/Caso** - Média de doutrinas citadas por caso. 
    - Tamanho: 20%
2. **Legislações/Caso** - Média de citações de legislação por caso.
    - Tamanho: 20%
3. **Acordãos/Caso** - media  de acordãos citados por caso.
    - Tamabnho:20%
4. **Autores mais citados** - Gráfico em formato de pizza, contendo os 6 autores mais citados em todo o período.
    - Tamanho: 40%


### LINHA 3

Em uma linha, deverá ser exibido o heatmap (tabela matricial) Autores x Ministro:

1. Eixo X: Ministro.
    - Limitar aos 10 ministros que mais realizaram citações doutrinárias.
    - Exibir a primeira letra do nome do ministro, e último Sobrenome. Ex.: Gilmar Mendes -> G. Mendes, Carmem Lúcia -> C. Lúcia, etc.
    - Exibir os nomes com o texto inclinado em 45º para cima, para otimizar o uso do espaço disponível.
    - Disponibilzar link para detalhes do ministro no nome.
2. Eixo Y: Autor.
    - Limitar aos 15 autores mais citados.
    - Abreviar os primeiros nomes e incluir sobrenome, ex.:  Luis Roberto Barroso - > L. S. Barroso,  José Joaquim Gomes Canotilho -> J. J. G. Canotilho, etc.
    - Disponibilzar link para detalhes do autor no nome.
3. Células: Número de citações do autor (Y) feitas pelo ministro (X).
    - Utilizar escala de cores para representar o volume de citações, conforme padrão já utilizado em outras interfaces do sistema.

---

### LINHA 4

Em uma linha, deverão ser exibidos os dois cards descritos abaixo, dividindo em proporção 50% para cada card:

1. **Autores** - Tabela com os 10 autores mais citados, exibindo as colunas:
    - Autor (com link para detalhes do autor)
    - Número de citações
    - Percentual de citações (em relação ao total de citações)
    - Tamanho: 50%
    - Diponibilizar link no nome do autor para detalhes do autor.
    - Disponibilizar opção "ver mais" para carregar mais autores.
2. **Obras** - Tabela com as 10 obras mais citadas, exibindo as colunas:
    - Título da obra (com link para detalhes da obra) truncar os títulos para o tamanho do card, se necessário, incluir reticências e tooltip com o título completo.
    - Link para detalhes da obra
    - Autor (nome abreviado, com link para detalhes do autor)
    - Número de citações
    - disponibilizar opção "ver mais" para carregar mais obras.

### Linha 5
Em uma linha, deverão ser exibidos os dois cards descritos abaixo, dividindo em proporção 50% para cada card:

1. **Legislações** - Tabela com as 10 legislações mais citadas, exibindo as colunas:
    - Legislação: (com link para detalhes da legislação) raw da legislção citada
    - Número de citações
    - Tipo de legislação (Constituição, Código, Lei, etc)
    - Disponibilizar opção "ver mais" para carregar mais legislações.
2. **Acordãos** - Tabela com os 10 acordãos mais citados, exibindo as colunas:
    - Número do acordão (com link para detalhes do acordão)
    - Número de citações
    - Ano do acordão
    - Disponibilizar opção "ver mais" para carregar mais acordãos.





    ## INTERFACE PRINCIPAL MINISTROS

    Renomear a interface de "Interface 2: Ministros" para "Análise de Ministros".

    A interface de análise de ministros deverá conter os seguintes cards e elementos.:

    1. Bloco de Filtros
        - Filtros por:
            - Ministro
            - Classe processual
            - Ano de julgamento
            - Relator
            - intervalo de datas

    2. BIG NUMBERS (INDICADORES PRINCIPAIS)
    - Em uma única linha, exibir os seguintes indicadores, dividindo o espaço igualmente entre os cards:
        - Total de Ministros
        - Média de casos por Ministro
        - Doutrinas por Ministro (média de doutrinas citadas por ministro em cada caso)
        - Legislações por Ministro (média de legislações citadas por ministro em cada caso)
        - Acordãos por Ministro (média de acordãos citados por ministro em cada caso)
    
    3. Métricas de Atuação dos Ministros
    - Em uma linha, exibir os seguintes cards, dividindo o espaço igualmente entre eles
        - Ministros mais Ativos: Gráfico de barras com os 10 ministros que mais atuaram como relatores, exibindo o número total de casos relatados por cada um. 
        - Relatorias por Ministro: Gráfico de rosca com os 7 ministros que mais atuaram como relatores, exibindo o percentual de casos relatados por cada um.
    
    4. Tabela detalhada de Ministros
    - Exibir uma tabela com os ministros, contendo as seguintes colunas:
        - Nome do Ministro (com link para detalhes do ministro), abreviar os primeiros nomes e incluir sobrenome, ex.:  Luis Roberto Barroso - > L. S. Barroso,  José Joaquim Gomes Canotilho -> J. J. G. Canotilho, etc.
        - Casos - Número total de casos em que o ministro atuou como relator ou como participante0.
        - Relatorias - Número total de casos em que o ministro atuou como relator.
        - Casos/Ano - Média de casos por ano em que o ministro atuou como relator ou participante.
        - Citado - Noúmero total de casos em que o ministro foi citado em decisões, doutrinas, legislações, etc. Ao passar o mouse no número Exibir tooltip com o detalhamento das citações (ex.: 10 citações em decisões, 5 citações em doutrinas, etc).
        - Autores (incluir tooltip, informando que é o núemro total de autores diferentes citados por ele) - Número total de autores únicos citados em casos relacionados ao ministro. 
    



    ## INTERFACE DETALHES MINISTRO

    lIMITAR TODOS OS CARDS À EXIBIÇÃO DE 10 ITENS, COM OPÇÃO DE "VER MAIS" PARA CARREGAR MAIS ITENS.
    iNCLUIR tooltips nos nomes de cada card, explicando o que cada métrica representa.

    