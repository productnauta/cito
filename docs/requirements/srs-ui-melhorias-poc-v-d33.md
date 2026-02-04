# SRS — Melhorias Mínimas nas Interfaces Web do Sistema (poc-v-d33)

## 1. Objetivo

Definir os requisitos funcionais e de interface necessários para corrigir inconsistências, padronizar navegação por entidades jurídicas e melhorar a usabilidade das interfaces web do sistema.

---

## 2. Escopo

Este documento cobre as seguintes melhorias:

1. Padronização global de **links contextuais** (processo, relator, obra, autor).
2. Correção de **totalizadores** na interface de **Resumo de Ministros**.
3. Melhoria de **exibição progressiva** no card **Partes do Processo**.

---

## 3. Requisitos Funcionais

### RF-01 — Links contextuais para Processos (escopo global)

**Descrição**
Sempre que um **título de processo** (ex.: “ADPF 54”) for exibido em qualquer interface web do sistema, ele deverá funcionar como um **link para a interface de detalhes do processo correspondente**.

**Regras**

- O link deve ser aplicado diretamente no texto do título do processo.
- A navegação deve ser interna ao sistema.
- A rota deve utilizar obrigatoriamente o identificador:
  - `identity.stfDecisionId`.

**Abrangência**

- Listas
- Tabelas
- Cards
- Cabeçalhos
- Resumos
- Breadcrumbs
- Dashboards

---

### RF-02 — Links contextuais para Relatores (escopo global)

**Descrição**
Sempre que o **nome de um relator/ministro** for exibido em qualquer interface, ele deverá ser clicável e direcionar para a **interface de detalhes do relator**.

**Regras**

- O link deve ser aplicado diretamente no nome do relator.
- A rota deve utilizar um identificador canônico do ministro (ex.: id interno ou slug normalizado).
- O comportamento deve ser consistente em todas as telas.

---

### RF-03 — Links contextuais para Obras Doutrinárias (escopo global)

**Descrição**
Sempre que o **título de uma obra** for exibido, ele deverá direcionar para a **interface de detalhes da obra**.

**Regras**

- O link deve estar no próprio título da obra.
- A navegação deve utilizar identificador único/canônico da obra.

---

### RF-04 — Links contextuais para Autores de Obras (escopo global)

**Descrição**
Sempre que o **nome de um autor** for exibido, ele deverá ser clicável e direcionar para a **interface de detalhes do autor**.

**Regras**

- O link deve estar no próprio nome do autor.
- Deve utilizar identificador único ou slug normalizado do autor.

---

### RF-05 — Tratamento de exceções para links contextuais

**Descrição**
Quando um elemento (processo, relator, obra ou autor) **não possuir identificador resolvível**, o sistema deve tratar o caso de forma segura.

**Regras**

- O texto não deve ser clicável.
- Não devem existir links quebrados ou rotas inválidas.
- O layout não pode ser afetado.
- Opcional: exibir estado visual neutro/desabilitado.

---

### RF-06 — Correção dos totalizadores na interface “Resumo de Ministros”

**Descrição**
Na interface de **Resumo de Ministros**, o card que exibe a tabela de ministros com ocorrências deve apresentar corretamente os totalizadores atualmente exibidos como zero.

**Totalizadores afetados**

- Total de votos vencidos
- Total de votos definidos
- Total de votos indefinidos

**Regras**

- Os valores devem refletir corretamente os dados existentes no banco.
- As queries/aggregations devem ser revisadas e corrigidas.
- Os filtros aplicados devem ser consistentes com os dados exibidos na tabela.

---

### RF-07 — Consistência entre totalizadores e dados exibidos

**Descrição**
Os totalizadores exibidos no resumo devem ser coerentes com os dados efetivamente retornados para os ministros listados.

**Regras**

- Não é permitido exibir totalizadores zerados quando existirem ocorrências.
- Quando aplicável, os valores devem corresponder à soma lógica das ocorrências.

---

### RF-08 — Limitação inicial de exibição no card “Partes do Processo”

**Descrição**
Na interface de **detalhes do processo**, o card **Partes do Processo** deve exibir inicialmente apenas as **10 primeiras partes**.

**Regras**

- A ordenação deve respeitar a ordem atual definida pelo sistema.
- Caso existam até 10 partes, todas devem ser exibidas.

---

### RF-09 — Opção “Exibir mais” no card “Partes do Processo”

**Descrição**
Quando existirem mais de 10 partes, o sistema deve disponibilizar a opção **“Exibir mais”**.

**Regras**

- O controle “Exibir mais” deve aparecer apenas se houver mais de 10 partes.
- Ao ser acionado, todas as partes restantes devem ser carregadas e exibidas.
- Não deve haver duplicação ou perda de itens.

**Observação**

- O carregamento pode ser:
  - via backend (nova requisição), ou
  - via frontend (dados já carregados), conforme arquitetura existente.

---

## 4. Requisitos Não Funcionais

### RNF-01 — Consistência de UX

- O comportamento de links e expansão deve ser uniforme em todas as interfaces.

### RNF-02 — Acessibilidade e usabilidade

- Links devem respeitar padrões visuais do sistema.
- O comportamento nativo do navegador deve ser preservado (ex.: abrir em nova aba).

### RNF-03 — Segurança de navegação

- Nenhuma rota inválida deve ser exposta ao usuário.
- Links só devem ser renderizados quando houver identificador válido.

---

## 5. Critérios de Aceite (Consolidados)

1. **Processos**
   - Clicar em qualquer título de processo abre a tela correta do processo via `identity.stfDecisionId`.

2. **Relatores**
   - Clicar em qualquer nome de relator abre a tela correta de detalhes do relator.

3. **Obras e Autores**
   - Obras e autores exibidos são sempre navegáveis quando possuem identificador válido.

4. **Resumo de Ministros**
   - Totalizadores não permanecem zerados quando há dados.
   - Valores exibidos são coerentes com a base de dados.

5. **Partes do Processo**
   - Até 10 partes: exibição completa sem botão adicional.
   - Mais de 10 partes: exibição parcial + botão “Exibir mais”.
   - Após expansão: todas as partes visíveis corretamente.
