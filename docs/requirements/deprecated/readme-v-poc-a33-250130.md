# CITO - Radar de Jurisprudência do STF

O **CITO** é uma solução integrada para monitoramento, processamento e análise de jurisprudências do Supremo Tribunal Federal (STF), com foco especial em decisões que consolidam entendimentos relevantes. O sistema automatiza a coleta, estruturação e análise de decisões, extraindo metadados jurídicos e citações doutrinárias, e oferece dashboards e filtros avançados para pesquisa e inteligência jurídica.

![Visão Geral do Projeto CITO](cito-visao-discovery-full.png)

## Funcionalidades Principais

- **Monitoramento Contínuo do STF:** Coleta diária e retroativa de sentenças e jurisprudências, incluindo o inteiro teor e anexos.
- **Gestão Documental Automatizada:** Download, arquivamento e OCR de documentos originais (HTML, PDF, imagens).
- **Estruturação Jurídica:** Extração de metadados como número do processo, classe (ADI, ADC, ADPF, ADO), relator, órgão julgador, datas e partes envolvidas.
- **Extração e Normalização de Citações Doutrinárias:** Identificação de autores, obras, artigos, livros e normalização dos dados.
- **Banco de Dados Jurídico-Doutrinário:** Estrutura relacional entre decisões, citações, obras e autores, com rastreabilidade completa.
- **Busca e Pesquisa Avançada:** Indexação full-text, filtros combinados e consultas estruturadas.
- **Dashboards e Analytics:** Visualização de indicadores, rankings, evolução temporal e distribuição estatística.
- **Alertas e Integrações:** Notificações automáticas, API REST e exportação de dados em CSV, JSON e PDF.

## Modelo de Dados

- **Metadados das Decisões:** Número do processo, classe, órgão julgador, relator, datas, partes envolvidas.
- **Citações Doutrinárias:** Autores, obras/artigos/livros, normalização (autor, obra, ano, edição, páginas).
- **Rastreabilidade Completa:** Decisões ↔ Documentos ↔ Obras ↔ Autores.

## Fontes de Jurisprudência

- **Base Histórica:** Scraping retroativo de decisões anteriores.
- **Novas Jurisprudências:** Monitoramento e coleta contínua de novas publicações.

## Dashboard de Insights Jurídicos

- Artefatos, indicadores e big numbers.
- Relatórios customizáveis.
- Filtros avançados por classe, relator, período, órgão julgador, autor e obra.

## MVP

- Coleta e armazenamento de decisões dos últimos 6 meses.
- Indexação de metadados essenciais.
- Consulta simples por número de processo e relator.
- Extração experimental de citações doutrinárias.
- Dashboard mínimo com volume por classe, ranking de relatores e autores citados.

## Estrutura do Projeto

- [`discovery/`](discovery) - Documentação, briefing, fluxos e modelagem.
- [`mockups/`](mockups) - Mockups de telas e fluxos.
- [`poc/`](poc) - Prova de conceito e objetivos do projeto.

## Referências

- [Portal de Jurisprudência do STF](https://jurisprudencia.stf.jus.br/pages/search?base=acordaos)

---

#  Estimativa de Esforço – MVP CITO (Piloto)

## Estimativa Detalhada

| Bloco                        | Atividade                                                      | Horas |
| ---------------------------- | -------------------------------------------------------------- | ----- |
| **Backend / Infraestrutura** | Configuração ambiente (N8N, Docker, FastAPI, PostgreSQL, MongoDB) | 16h   |
|                              | Integração n8n → backend (coleta → API/storage) e API básica de consulta (processo, classe, relator, datas) | 12h   |
| **Scraping e Processamento** | n8n Workflow para ingestão contínua do STF                     | 16h   |
|                              | Scraping retroativo (últimos 6 meses)                          | 8h    |
|                              | OCR (Tesseract) + Conversão texto (Docling/PDFPlumber)         | 6h    |
| **Extração de Metadados**    | Parser de decisões (processo, classe, relator, órgão, datas)   | 16h   |
|                              | Armazenamento no banco                                         | 12h   |
| **Citações Doutrinárias**    | Pipeline inicial Regex + heurísticas                           | 6h    |
|                              | Estrutura mínima de rastreabilidade (autor, obra, ano, página) | 10h   |
|                              | Persistência no banco (AUTHOR/WORK/CITATION)                   | 6h    |
| **Frontend (MVP)**           | Login básico                                                   | 4h    |
|                              | Tela de pesquisa                                               | 8h    |
|                              | Visualização do inteiro teor + download PDF                    | 6h    |
|                              | Tela de citações doutrinárias                                  | 6h    |
|                              | Dashboard piloto (indicadores + exportação CSV/PDF)            | 8h    |
| **Dashboard / Relatórios**   | Relatório básico                                               | 12h   |

---

## Total de Esforço Estimado
**152 horas** de desenvolvimento.

## Observações
- Estimativas focadas no escopo do MVP para prova de conceito.  
- Horas não incluem atividades de gestão de projeto, QA detalhado ou suporte pós-MVP.  
- O escopo de scraping retroativo está limitado a **6 meses de decisões do STF**.  
- O frontend será básico, utilizando **MUI sem personalizações**.
