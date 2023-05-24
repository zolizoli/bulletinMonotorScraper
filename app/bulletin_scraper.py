import re
from datetime import datetime
from io import BytesIO

import pdfplumber
import requests
import spacy
from bs4 import BeautifulSoup
from hun_date_parser import text2datetime
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert
from sqlalchemy.sql import text

########################################################################################################################
#####                                                    setup db                                                  #####
########################################################################################################################
engine = create_engine(
    "sqlite:///bulletin.db", pool_recycle=10000, echo=False, future=True
)
connection = engine.connect()
metadata = MetaData()

bulletins = Table(
    "bulletins",
    metadata,
    Column("bulletin_id", Integer(), primary_key=True),
    Column("URI", String(300), nullable=False, unique=True),
    Column("scrape_date", String(50), nullable=False),
    Column("doc_date", String(50)),
    Column("issue", String(100)),
    Column("text", String(1000000000), index=True),
    Column("lemmatized", String(1000000000), index=True),
)

metadata.create_all(engine)
connection.close()

##### NLP
nlp = spacy.load("hu_core_news_lg")


########################################################################################################################
#####                                    Scrape, extract and process bulletins                                     #####
########################################################################################################################
def get_a_month(month, year):
    download_links = []
    for i in range(1, 10):  # TODO: determine the longest possible range
        url = f"https://magyarkozlony.hu/?year={year}&month={month}&serial=&page={i}"
        try:
            html = requests.get(url, verify=False).text
            soup = BeautifulSoup(html, "html.parser")
            all_links = soup.find_all("a")
            download_links.extend(
                [
                    e["href"]
                    for e in all_links
                    if "hivatalos-lapok" in e["href"] and "dokumentumok" in e["href"]
                ]
            )
        except Exception as e:
            print("out of range >>>>", e)
            return set(download_links)
    for dl in download_links:
        try:
            res = requests.get(dl, verify=False).content
            with pdfplumber.open(BytesIO(res)) as pdf:
                text = []
                for page in pdf.pages:
                    text.append(page.extract_text())
                text = "\n".join(text)
                scrape_date = datetime.now().strftime("%Y/%m/%d")
                doc_date = re.findall(
                    r"[0-9]{4}\. [a-zA-záéöőúüűÁÉÖŐÜŰ]+ \d{1,2}\.", text
                )[0]
                doc_date = text2datetime(doc_date)[0]["start_date"].strftime("%Y/%m/%d")
                issue = re.findall(r"\d{1,3}\. szám", text)[0]
                lemmas = []
                doc = nlp(text)
                for t in doc.sents:
                    for e in t:
                        if e.pos_ in ["ADJ", "ADV", "VERB"] and e.lemma_.isalpha():
                            lemmas.append(e.lemma_.lower())
                lemmas = " ".join(lemmas)
                try:
                    conn = engine.connect()
                    ins = insert(bulletins).values(
                        URI=dl,
                        scrape_date=scrape_date,
                        doc_date=doc_date,
                        issue=issue,
                        text=text,
                        lemmatized=lemmas,
                    )
                    res = conn.execute(ins)
                    conn.commit()
                    conn.close()
                except Exception as e:
                    print("\tINSERTION ERROR", e)
                    try:
                        conn.close()  # ugly but we have to release the connection
                    except Exception as e:
                        pass
                    continue
        except Exception as e:
            print("REQUEST ERROR", e)
            continue
    return "Done"
