

Analise o script abaixo, e identifique o formato e estrutura do documento gravado na collection. 

Responda com a descrição breve do que é realizado pelo script e quais dados atualmente estão sendo obtidos, extraídos e em qual campo/estrutura eles estão sendo armazenados.

# e_fetch_case_html.py

"""
e_fetch_case_html.py

Atualizações solicitadas:
- Utiliza o schema atual da collection "case_data" (com agrupadores).
- Busca o próximo documento apto baseado em:
    - identity.stfDecisionId
    - stfCard.caseUrl
    - status.sourceStatus / status.pipelineStatus
- Grava o HTML obtido em:
    caseContent.originalHtml
- Se caseContent.originalHtml já existir, atualiza o conteúdo (sempre sobrescreve).
- Mantém lock/claim atômico para evitar concorrência:
    status.pipelineStatus: extracted -> caseScraping
- Em caso de sucesso:
    - caseContent.originalHtml
    - processing.caseHtmlScrapedAt (UTC)
    - status.pipelineStatus: caseScraped
- Em erro:
    - processing.caseHtmlError
    - processing.caseHtmlScrapedAt (UTC)
    - status.pipelineStatus: caseScrapeError

Observação:
- Playwright é o método principal (evita SSL issues no Codespaces).
- requests é opcional via env USE_REQUESTS_FIRST=true
"""

import asyncio
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

import certifi
import requests
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


## ------------------------------------------------------------
## Mongo (fixo) [recomendado migrar para ENV]
## ------------------------------------------------------------
MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
DB_NAME = "cito-v-a33-240125"
COLLECTION = "case_data"

## Pipeline status (schema atual)
PIPELINE_INPUT = "listExtracted"   ## ou "extracted" (fallback) — ver claim()
PIPELINE_PROCESSING = "caseScraping"
PIPELINE_OK = "caseScraped"
PIPELINE_ERROR = "caseScrapeError"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


USE_REQUESTS_FIRST = _env_bool("USE_REQUESTS_FIRST", False)
SSL_VERIFY = _env_bool("STF_SSL_VERIFY", True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


## ------------------------------------------------------------
## Mongo helpers
## ------------------------------------------------------------
def get_collection() -> Collection:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION]


def _get_stf_decision_id(doc: Dict[str, Any]) -> Optional[str]:
    v = doc.get("identity", {}).get("stfDecisionId")
    if isinstance(v, str) and v.strip() and v.strip() != "N/A":
        return v.strip()
    return None


def _get_case_url(doc: Dict[str, Any]) -> Optional[str]:
    v = doc.get("stfCard", {}).get("caseUrl")
    if isinstance(v, str) and v.strip() and v.strip() != "N/A":
        return v.strip()
    return None


def claim_oldest_extracted(col: Collection) -> Optional[Dict[str, Any]]:
    """
    Claim atômico do documento mais antigo apto para scraping.

    Critérios:
    - status.pipelineStatus em um dos estados de entrada
      (PIPELINE_INPUT ou "extracted" para compat)
    - identity.stfDecisionId válido
    - stfCard.caseUrl válido
    - SEMPRE permite atualizar caseContent.originalHtml (se existir, atualiza)
      (logo, não filtra por existência de originalHtml)
    """
    return col.find_one_and_update(
        {
            "status.pipelineStatus": {"$in": [PIPELINE_INPUT, "extracted"]},
            "identity.stfDecisionId": {"$exists": True, "$nin": [None, "", "N/A"]},
            "stfCard.caseUrl": {"$exists": True, "$nin": [None, "", "N/A"]},
        },
        {
            "$set": {
                "status.pipelineStatus": PIPELINE_PROCESSING,
                "processing.caseHtmlScrapingAt": utc_now(),
            }
        },
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def mark_success(col: Collection, doc_id, *, html: str) -> None:
    """
    Grava/atualiza:
    - caseContent.originalHtml (sempre sobrescreve)
    - processing.caseHtmlScrapedAt
    - status.pipelineStatus
    Limpa erro anterior, se existir.
    """
    col.update_one(
        {"_id": doc_id, "status.pipelineStatus": PIPELINE_PROCESSING},
        {
            "$set": {
                "caseContent.originalHtml": html,
                "processing.caseHtmlScrapedAt": utc_now(),
                "status.pipelineStatus": PIPELINE_OK,
                "processing.caseHtmlError": None,
            }
        },
    )


def mark_error(col: Collection, doc_id, *, error_msg: str) -> None:
    col.update_one(
        {"_id": doc_id, "status.pipelineStatus": PIPELINE_PROCESSING},
        {
            "$set": {
                "processing.caseHtmlError": error_msg,
                "processing.caseHtmlScrapedAt": utc_now(),
                "status.pipelineStatus": PIPELINE_ERROR,
            }
        },
    )


## ------------------------------------------------------------
## requests (opcional)
## ------------------------------------------------------------
def fetch_html_requests(url: str) -> Tuple[str, int]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://jurisprudencia.stf.jus.br/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    verify_opt = certifi.where() if SSL_VERIFY else False
    if not SSL_VERIFY:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    resp = requests.get(url, headers=headers, timeout=60, verify=verify_opt)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text, resp.status_code


## ------------------------------------------------------------
## Playwright (principal)
## ------------------------------------------------------------
async def fetch_html_playwright(url: str) -> str:
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        raise RuntimeError(
            "Playwright não disponível. Instale com: pip install playwright && playwright install"
        ) from e

    from contextlib import suppress

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=USER_AGENT,
            extra_http_headers={"accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="networkidle", timeout=60_000)
            await page.wait_for_timeout(3000)
            return await page.content()

        except (asyncio.CancelledError, KeyboardInterrupt):
            raise

        finally:
            with suppress(Exception):
                await page.close()
            with suppress(Exception):
                await context.close()
            with suppress(Exception):
                await browser.close()


## ------------------------------------------------------------
## Main
## ------------------------------------------------------------
async def main() -> int:
    col: Optional[Collection] = None
    doc_id = None

    try:
        col = get_collection()
        doc = claim_oldest_extracted(col)
        if not doc:
            print("Nenhum documento elegível para scraping (pipelineStatus em estado de entrada).")
            return 0

        doc_id = doc["_id"]
        stf_id = _get_stf_decision_id(doc)
        case_url = _get_case_url(doc)

        print("📄 Documento selecionado para scraping:")
        print(f"   _id: {doc_id}")
        print(f"   stfDecisionId: {stf_id}")

        if not stf_id or not case_url:
            mark_error(col, doc_id, error_msg="Documento inválido: identity.stfDecisionId ou stfCard.caseUrl ausente/N/A")
            return 1

        ## checagem solicitada (apenas informativa): se já existe, vamos atualizar
        existing_html = doc.get("caseContent", {}).get("originalHtml") if isinstance(doc.get("caseContent"), dict) else None
        if isinstance(existing_html, str) and existing_html.strip():
            print("ℹ️ caseContent.originalHtml já existe: será atualizado (sobrescrito).")
        else:
            print("ℹ️ caseContent.originalHtml não existe: será criado.")

        html = ""

        if USE_REQUESTS_FIRST:
            try:
                print("🌐 Buscando HTML via requests...")
                html, http_status = fetch_html_requests(case_url)
                print(f"📶 HTTP {http_status} | HTML len={len(html)}")
            except Exception as e:
                print(f"⚠️ requests falhou ({e}). Tentando Playwright...")
                html = ""

        if not html:
            print("🌐 Buscando HTML via Playwright...")
            html = await fetch_html_playwright(case_url)
            print(f"✅ Playwright HTML len={len(html)}")

        mark_success(col, doc_id, html=html)
        print(f"🗃️ Atualizado no MongoDB: pipelineStatus='{PIPELINE_OK}' (caseContent.originalHtml gravado)")
        return 0

    except PyMongoError as e:
        msg = f"Erro MongoDB: {e}"
        print(f"❌ {msg}")
        if col is not None and doc_id is not None:
            mark_error(col, doc_id, error_msg=msg)
        return 2

    except Exception as e:
        msg = str(e)
        print(f"❌ Erro: {msg}")
        if col is not None and doc_id is not None:
            mark_error(col, doc_id, error_msg=msg)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    print(f"Exit code: {exit_code}")


---





# SANITIZAR CASE HTML

Implemente no código e_fetch_case_html.py os seguintes novos recursos:

- Obter o conteúdo do campo 'caseContent.originalHtml'.
    - Verificar o tamanho em kb do conteúdo html original. será utilziado para informar o usuário no terminal.
- Realizar a sanitização e conversão para markdown.
    - Deve utilizar a bilioteca 'python-markdownify' para realizar a conversão.
    - Devem ser removidos todos os códigos html, scripts, css e outros, extraíndo apenas o conteúdo de texto.
    - Deve ser preservados os elementos de formatação como itálico, negrito, sublinhado, parágrafo, etc. Tal formatação deve ser convertida pela sua equivalente em linguagem markdown.
    - Os links existentes no texto deverão ser preservados.
    - O texto final, deverá ser o conterúdo textual, em formato markdown, preservando a estrutura de títulos, tópicos, listas e formatação.
    - Verificar o tamanho em kb do conteúdo convertido. será utilziado para informar o usuário no terminal.
- O conteúdo convertido em markdown, deverá ser salvo no respectivo documento, no campo 'caseContent.contentMd'.
- Após a conclusão, o campo 'audit.sourceStatus' deverá ser atualizado com o status do processamento.
- OBTENHA detalhes sobre os itens que serão processados (total de itens, total de novos) e solicite ao usuário qual ação deseja executar:

    1 - processar todos os itens (atualizando os documentos com as novas informações)
    2 - processar apenas os novos itens
    3 - atualizar itens (atualiza as informações apenas dos itens já processsados)
    IMPORTANTE: Ao selecionar qualquer opção, o sistema dever solicitar se deseja processar 1 item por vez, com confirmação para processar o proximo item, ou se deseja processar todos automaticamente sem confirmação.

 Exiba para o usuários as seguintes informações sobre o andamento da exeução, 

exemploS:

"""
-------------------------------------
OBTER E SANITIZAR HTML DAS DECISÕES
-------------------------------------
Total de itens: xxx
Novos: xxxx
Existentes: xxxx

-------------------------------------
ESCOLHA UMA OPÇÃO
-------------------------------------
1 - PROCESSAR TUDO
2 - PROCESSAR NOVOS
3 - ATUALIZAR EXISTENTES

-------------------------------------


---

Mensagens de status

-------------------------------------
PROCESSAMENTO INICIADO - ITENS 23
-------------------------------------

Item 1/23: _id
Processo: stfCard.caseTitle_

Obter HTML da decisão:          OK 
Tamanho html:                   XXX kb
Gravar HTML original:           OK
Converter para Markdown:        OK
Tamanho markdown:               xxx kb
Gravar markdown:                OK

PROCESSAMENTO ITEM FINALIZADO

---------------------------------------

Item 2/23: _id
Processo: stfCard.caseTitle_

Obter HTML da decisão:          OK 
Tamanho html:                   XXX kb
Gravar HTML original:           OK
Converter para Markdown:        OK
Tamanho markdown:               xxx kb
Gravar markdown:                OK

PROCESSAMENTO ITEM FINALIZADO



# MINER
Para cada processo, Obtenha o conteúdo do campo 'caseContent.caseMarkdown' identifique no conteúdo os seguintes títulos (identificados com ####) e extraia todo o conteúdo existente.


- Publicação
- Partes
- Ementa
- Decisão
- Indexação
- Legislação
- Observação
- Doutrina

Utilize a seguinte regra de estrutura e nomenclatura de dados:

Título/seção - Nome do campo na no documento na collection "case_data"

Publicação	caseContent.casePublication - 
Partes	    caseContent.caseParties
Ementa	    caseContent.caseSummary - 
Decisão	    caseContent.caseDecision    - 
Indexação	caseContent.caseKeywords    - 
Legislação	caseContent.caseLegislation - 
Observação	caseContent.caseNotes   - 
Doutrina	caseContent.caseDoctrine    - 


Caso exista algum título não identificado acima, o mesmo deve ser inserido com seu próprio nome.
AR DADOS caseContent


# MINERAR COM IA - DOUTRINA

Utilizar modelo de IA Mistral via api, para analisar o conteúdo obtido do banco de dados collection 'case_data' campo caseData.caseDoctrine, identificar, extraír e organizar as referencias.

- 
- Identificar cada citação individual (normalmente deparadas por vírgula)
- Identificar e extrair detalhes de cada citação:
    - Nome e sobrenome do autor 
    - Nome do documento, publicação, livro obra, peça ou referência citada
    - Edição
    - Local de publicação
    - Editora
    - Ano
    - Página citada
- EXEMPLO:
    - citação identificada: BARROSO, Luís Roberto. O controle de constitucionalidade no direito brasileiro: exposição sistemática da doutrina e análise crítica da jurisprudência. 4. ed. São Paulo: Saraiva, 2009. p. 181.
    - dados extraídos
        - autor: BARROSO, Luís Roberto
        - publicação: O controle de constitucionalidade no direito brasileiro: exposição sistemática da doutrina e análise crítica da jurisprudência.
        - edição: 4 ed
        - Local publicação: São paulo
        - Ano: 2009
        - Página: 181

Exemplo do conteúdo que deve ser analizado:
#### Doutrina

ALEXY, Robert. Teoria dos direitos fundamentais. 2. ed. Trad. Virgílio Afonso da Silva. São Paulo: Malheiros, 2015, p. 582.
CANOTILHO, José Joaquim Gomes. Direito constitucional . 6. ed. Coimbra: Almedina, 1993, p. 139.
MENDES, Gilmar Ferreira; BRANCO, Paulo Gustavo Gonet. Curso de direito constitucional. 17. ed. São Paulo: SaraivaJur, 2022, p. 233-234 e 1.561. FORSTHOFF, Ernst. Lo Stato della società industriale. Ed. de Alessandro Mangia. Milão: Ed. Giuffrè, 2011, p. 161.
LIMA, Tulius Marcus Fiuza. Direito à licença maternidade para casais homoafetivos femininos. p. 473-489. In: Direito Econômico e Desenvolvimento. Entre a prática e a academia. Coordenador. Cláudio Xavier Seefelder Filho. Belo Horizonte: Forum. 2023.
ZAGREBELSKY, Gustavo; MARCENÒ, Valeria. Giustizia Costituzionale. Bolonha: il Mulino, 2012, p. 337-420.




adicione ao script, mais uma funcionalidade após as existentes, para que ele obtenha o conteúdo de caseData.caseLegislation, analise e identifique individualmente cada uma das citações de legislação.

Formato: As citações seguem um padrão estruturado com prefixos:

LEG-FED → Legislação Federal

LEG-EST → Legislação Estadual

CF → Constituição Federal

EMC → Emenda Constitucional

ART → Artigo

INC → Inciso

PAR → Parágrafo

LET → Letra

Separadores: As citações são identificáveis por:

Quebras de linha (principal)

Espaços entre referências diferentes

Mudança de tipo de norma (ex: de ART para INC)

Citações compostas: Algumas linhas contêm múltiplas referências, mas devem ser tratadas como citações individuais (ex: "INC-00047 LET-E" é uma única referência a um inciso e sua letra específica).

Normas principais: Além das citações específicas, o texto referencia normas completas:

CONSTITUIÇÃO FEDERAL (CF-1988)

EMC-000229/2019

LEI-008112/1990 (RJU)

LEI-011770/2008

LEI-014457/2022

DEC-006690/2008

PJL-000139/2022

LCP-000046/1994 (ES)

LCP-000855/2017 (ES)


As informações deverão ser salvas em caseData.caseLegislationReferences de forma estruturada.
 # 

---
Utilizando o código dos seguintes scripts: b_search_save_html-old.py 
#


{
  "_id": "65f0c9f0e1b2c3d4e5f67890",
  "caseStfId": "sjur12345",
  "caseIdentification": {
    "caseTitle": "ADI 7518 / ES - ESPÍRITO SANTO",
    "caseClassDetail": "ADI",
    "caseCode": "7518",
    "judgingBody": "Tribunal Pleno",
    "rapporteur": "Min. Gilmar Mendes",
    "caseUrl": "https://jurisprudencia.stf.jus.br/..."
  },
  "dates": {
    "judgmentDate": "16/09/2024",
    "publicationDate": "02/10/2024"
  },
  "caseContent": {
    "caseHtml": "<html>...</html>",
    "caseHtmlClean": "<div class=\"mat-tab-body-wrapper\">...</div>",
    "caseMarkdown": "#### Publicação\n..."
  },
  "rawData": {
    "rawPublication": "PROCESSO ELETRÔNICO\nDJe-s/n DIVULG 01-10-2024 PUBLIC 02-10-2024",
    "rawParties": "REQTE.(S): PROCURADORA-GERAL DA REPÚBLICA\nINTDO.(A/S): GOVERNADOR DO ESTADO DO ESPÍRITO SANTO",
    "rawSummary": "Ação direta de inconstitucionalidade. 2. Licença-parental...",
    "rawDecision": "Decisão ...",
    "rawKeywords": "NECESSIDADE, EXTINÇÃO, TRIBUNAL DO JÚRI, ...",
    "rawLegislation": "LEI-008112/1990 (RJU) ...",
    "rawNotes": "Observação ...",
    "rawDoctrine": "BARROSO, Luís Roberto..."
  },
  "caseData": {
    "caseParties": [
      { "partieType": "REQTE.(S)", "partieName": "PROCURADORA-GERAL DA REPÚBLICA" },
      { "partieType": "INTDO.(A/S)", "partieName": "GOVERNADOR DO ESTADO DO ESPÍRITO SANTO" }
    ],
    "caseKeywords": [
      "licença parental",
      "servidor público",
      "constitucionalidade"
    ],
    "caseDoctrineReferences": [
      {
        "author": "BARROSO, Luís Roberto",
        "publicationTitle": "O controle de constitucionalidade no direito brasileiro: exposição sistemática da doutrina e análise crítica da jurisprudência",
        "edition": "4 ed",
        "publicationPlace": "São Paulo",
        "publisher": "Saraiva",
        "year": 2009,
        "page": "181",
        "rawCitation": "BARROSO, Luís Roberto. O controle de constitucionalidade... p. 181."
      }
    ],
    "caseLegislationReferences": [
      {
        "jurisdictionLevel": "federal",
        "normType": "CF",
        "normIdentifier": "CF-1988",
        "normYear": 1988,
        "normDescription": "Constituição Federal",
        "normReferences": [
          {
            "articleNumber": 5,
            "isCaput": true,
            "incisoNumber": 3,
            "paragraphNumber": null,
            "isParagraphSingle": false,
            "letterCode": null
          }
        ]
      }
    ]
  },
  "processing": {
    "pipelineStatus": "enriched",
    "caseHtmlScrapedAt": "2026-01-26T22:10:00Z",
    "caseContentMinedAt": "2026-01-26T22:35:00Z",
    "caseDoctrineRefsAt": "2026-01-26T22:40:00Z",
    "caseLegislationRefsAt": "2026-01-26T22:41:00Z",
    "lastUpdatedAt": "2026-01-26T22:41:00Z",
    "errors": []
  },
  "status": {
    "pipelineStatus": "caseScraped"
  },
  "sourceIds": {
    "rawHtmlId": "65f0c9f0e1b2c3d4e5f11111"
  }
}
