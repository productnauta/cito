## PROJETO CITO

O sistema jurídico brasileiro, em especial no âmbito do Supremo Tribunal Federal (STF), gera diariamente uma grande quantidade de decisões, sentenças e documentos que, pela sua relevância, precisam ser monitorados, organizados e analisados de forma eficiente. O desafio está na dificuldade de acompanhar continuamente essas publicações, identificar as jurisprudências de interesse e estruturar informações ricas em metadados jurídicos e doutrinários.

Principais problemas:

* Alto volume e dispersão de publicações do STF.
* Dificuldade de acesso ao **inteiro teor** das decisões e documentos anexos.
* Ausência de estruturação adequada de metadados jurídicos.
* Complexidade na identificação de citações doutrinárias relevantes.
* Necessidade de dashboards e mecanismos de busca avançada.

---

## Conceituação do CITO

O **CITO** foi concebido como uma solução integrada de **monitoramento, processamento e análise de jurisprudências do STF**, com foco especial em decisões que consolidam entendimentos relevantes. Através de coleta automatizada, processamento documental, extração de dados e mineração de citações doutrinárias, o sistema permitirá transformar informações jurídicas complexas em **conteúdo estruturado, pesquisável e analítico**, apoiando juristas, pesquisadores e instituições na tomada de decisão.

---

## Principais Recursos e Funcionalidades

* **Monitoramento Contínuo do STF**

  * Coleta diária e retroativa de sentenças e jurisprudências.
  * Obtenção do inteiro teor e anexos associados.

* **Gestão Documental Automatizada**

  * Download e arquivamento de originais (HTML, PDF, imagens).
  * OCR para textos em imagens.
  * Preservação e rastreabilidade de documentos.

* **Estruturação Jurídica**

  * Identificação de metadados: número do processo, classe (ADI, ADC, ADPF, ADO), relator, órgão julgador, datas e envolvidos.
  * Organização de dados em modelo relacional robusto.

* **Extração e Normalização de Citações Doutrinárias**

  * Identificação de autores, obras e referências em sentenças.
  * Classificação e padronização de dados (autor, título, ano, edição).

* **Banco de Dados Jurídico-Doutrinário**

  * Integração entre decisões, citações, obras e autores.
  * Consultas estruturadas e rastreabilidade completa.

* **Busca e Pesquisa Avançada**

  * Indexação full-text (ElasticSearch/OpenSearch).
  * Filtros combinados por classe, relator, autor, obra e período.

* **Analytics e Dashboards Interativos**

  * Evolução temporal de citações e jurisprudências.
  * Ranking de autores e obras mais influentes.
  * Distribuição estatística por classe processual, relator ou órgão julgador.

* **Alertas e Integrações Externas**

  * Notificações automáticas sobre novas decisões e citações.
  * API REST para integração com sistemas jurídicos de terceiros.
  * Exportação em formatos CSV, JSON e PDF.

---

## Conclusão

O **Projeto CITO** representa um passo estratégico para transformar o monitoramento de jurisprudência em um processo **automatizado, inteligente e acessível**. Ao consolidar informações jurídicas complexas em uma plataforma integrada, o sistema amplia a **transparência, a eficiência na pesquisa e a análise doutrinária**, fornecendo suporte essencial para juristas, pesquisadores e instituições que necessitam acompanhar de perto a evolução da jurisprudência no STF.





---------




⚙️ Mecanismo de Obtenção / Scrap Retroativo
Fonte

Portal do STF (Jurisprudência e Inteiro Teor de Acórdãos/Decisões).

Estratégia do MVP

Implementar scraper retroativo limitado a 6 meses (intervalo definido para MVP).

Permitir busca retroativa configurável (ex.: datas entre “01/03/2025 a 31/08/2025”).

Guardar cache local dos PDFs/HTMLs para reprocessamento sem sobrecarregar a fonte.

🔎 Mineração e Extração de Metadados
Passos do Pipeline

Coleta do documento (PDF/HTML).

Parsing do texto (pdfminer, PyMuPDF ou BeautifulSoup dependendo da fonte).

Regex + heurísticas para extração dos metadados básicos:

Número do processo.

Classe processual.

Relator.

Órgão julgador.

Datas relevantes.

Inserção no modelo de dados.

Logs de qualidade: percentual de extração bem-sucedida vs. falhas (para refino posterior).

🚀 Escopo Funcional do MVP

Coletar e armazenar decisões do STF dos últimos 6 meses.

Indexar metadados essenciais (decisão, processo, relator, datas, classe).

Oferecer consulta simples por número de processo e relator.

Experimentar a extração de citações doutrinárias (mesmo que parcial).

Dashboard mínimo com:

Volume de decisões por classe processual.

Ranking de relatores.

Autores citados (se identificados).