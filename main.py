# main.py - Coletor PNCP com comportamento humano simulado

import requests
import sqlite3
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime

BASE_LIST_URL = "https://pncp.gov.br/app/editais?pagina={pagina}&tam_pagina=100&ordenacao=-data"
HEADERS = {
    "User-Agent": "WaveCloneBot/1.0 (contato@dominio.com)"
}

conn = sqlite3.connect("pncp_scraper.db")
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS editais (
        url TEXT PRIMARY KEY,
        cnpj TEXT,
        local TEXT,
        orgao TEXT,
        unidadeCompradora TEXT,
        modalidade TEXT,
        tipo TEXT,
        modoDisputa TEXT,
        registroPreco TEXT,
        fonteOrcamentaria TEXT,
        dataDivulgacao TEXT,
        situacao TEXT,
        dataInicioRecebimento TEXT,
        dataFimRecebimento TEXT,
        valorTotal TEXT,
        objetoDetalhado TEXT,
        itens TEXT,
        links_documentos TEXT,
        coletado_em TEXT
    )
''')
conn.commit()

def simular_comportamento_humano():
    tempo = random.uniform(6, 12)
    print(f"Aguardando {tempo:.2f}s para simular comportamento humano...")
    time.sleep(tempo)

def extrair_links_lista(html):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a['href']
        if "/visualizar-edital/" in href:
            links.append("https://pncp.gov.br/app" + href)
    return list(set(links))  # remover duplicatas

def detalhar_edital(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"Erro {resp.status_code} ao acessar {url}")
            return None
        soup = BeautifulSoup(resp.text, "html.parser")

        def get_text(label):
            tag = soup.find(string=label)
            return tag.find_next().get_text(strip=True) if tag else ""

        cnpj = get_text("CNPJ:")
        local = get_text("Local:")
        orgao = get_text("Órgão:")
        unidadeCompradora = get_text("Unidade Compradora:")
        modalidade = get_text("Modalidade:")
        tipo = get_text("Tipo:")
        modoDisputa = get_text("Modo de Disputa:")
        registroPreco = get_text("Registro de Preço:")
        fonteOrcamentaria = get_text("Fonte Orçamentária:")
        dataDivulgacao = get_text("Data da Publicação:")
        situacao = get_text("Situação:")
        dataInicioRecebimento = get_text("Data início recebimento propostas:")
        dataFimRecebimento = get_text("Data fim recebimento propostas:")
        valorTotal = get_text("Valor estimado:")
        objetoDetalhado = get_text("Objeto detalhado:")
        itens = get_text("Itens:")

        links_documentos = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".pdf") and "download" in href:
                links_documentos.append(href)

        return (cnpj, local, orgao, unidadeCompradora, modalidade, tipo, modoDisputa, registroPreco,
                fonteOrcamentaria, dataDivulgacao, situacao, dataInicioRecebimento, dataFimRecebimento,
                valorTotal, objetoDetalhado, itens, ", ".join(links_documentos))
    except Exception as e:
        print(f"Erro ao detalhar {url}: {e}")
        return None

def coletar_maio():
    for pagina in range(1, 11):  # ajustável conforme volume
        url = BASE_LIST_URL.format(pagina=pagina)
        print(f"Coletando página de listagem: {url}")
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"Erro ao acessar listagem: {resp.status_code}")
            continue

        links = extrair_links_lista(resp.text)
        for link in links:
            if cursor.execute("SELECT 1 FROM editais WHERE url = ?", (link,)).fetchone():
                print(f"Já coletado: {link}")
                continue
            simular_comportamento_humano()
            detalhes = detalhar_edital(link)
            if detalhes:
                hoje = datetime.utcnow().isoformat()
                cursor.execute("""
                    INSERT OR REPLACE INTO editais 
                    (url, cnpj, local, orgao, unidadeCompradora, modalidade, tipo, modoDisputa,
                     registroPreco, fonteOrcamentaria, dataDivulgacao, situacao, dataInicioRecebimento,
                     dataFimRecebimento, valorTotal, objetoDetalhado, itens, links_documentos, coletado_em)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (link, *detalhes, hoje))
                conn.commit()
                print(f"Coletado: {link}")

if __name__ == "__main__":
    coletar_maio()
