from datetime import datetime

from fastapi import BackgroundTasks, FastAPI
from fastapi_health import health
from fastapi_utils.tasks import repeat_every
from pydantic import BaseModel
from sqlalchemy import (
    insert,
    select,
)
from sqlalchemy.sql import text

from app.bulletin_scraper import get_a_month, engine, metadata, bulletins

app = FastAPI()
btasks = BackgroundTasks()
metadata.create_all(engine)


class ScrapeDate(BaseModel):
    day: int
    month: int
    year: int


class Query(BaseModel):
    term1: str
    term2: str
    term3: str
    op1: str
    op2: str
    queryt: str


@app.get("/")
async def root():
    """The root enpoint has not been implemented."""
    return {"message": "there is nothing here, please post a msg to /scrape_article"}


@app.post("/search_by_issue_date")
def search_by_issue_date(scrapeDate: ScrapeDate):
    """Search bulletins by issue date.
    Returns every item issued on the given date"""
    conn = engine.connect()
    date = "/".join([str(scrapeDate.year), str(scrapeDate.month).zfill(2), str(scrapeDate.day).zfill(2)])
    s = select([bulletins]).where(bulletins.c.doc_date==date)
    rp = conn.execute(s)
    records = rp.fetchall()
    conn.close()
    return {"query": date,
            "results": list(records)}


@app.post("/search_by_scrape_date")
def search_by_scrape_date(scrapeDate: ScrapeDate):
    """Search bulletins by scrape date.
    Returns every item scraped on the given date."""
    conn = engine.connect()
    date = "/".join([str(scrapeDate.year), str(scrapeDate.month).zfill(2), str(scrapeDate.day).zfill(2)])
    s = select([bulletins]).where(bulletins.c.scrape_date == date)
    rp = conn.execute(s)
    records = rp.fetchall()
    conn.close()
    return {"query": date,
            "results": list(records)}


@app.post("/search_by_term")
def search_by_term(query: Query):
    """Search the full text field of the bulletins.
    Use simple matching and Boolean queries.
    TO BE IMPLEMENTED
    """
    search_string = text(f"select *, rank from FULLTEXTS where text MATCH {query.term1}")


@app.get("/scrape_now")
def scrape_now():
    """Returns only acknowledgement since this starts a background job"""
    month = datetime.now().month
    year = datetime.now().year
    btasks.add_task(get_a_month, month, year)
    return {"msg": "working hard in the background"}


@app.post("/scrape_date")
def scrape_date(scrapeDate: ScrapeDate):
    """Returns only acknowledgement since this starts a background job"""
    btasks.add_task(get_a_month, scrapeDate.month, scrapeDate.year)
    return {"msg": "working hard in the background"}


@app.on_event("startup")
@repeat_every(seconds=60 * 60 * 24)
def scheduled_scraper():
    """Runs bulletin_scraper.get_a_month() once a day"""
    print("Hello-bello" * 30)
    month = datetime.now().month
    year = datetime.now().year
    get_a_month(month, year)
    return None


##### Health check
def pass_condition():
    # TODO: elaborate this function
    return {"autotagging": "online"}


def sick_condition():
    # TODO: add sick condition
    return False


app.add_api_route("/health", health([pass_condition, sick_condition]))
