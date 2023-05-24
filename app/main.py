from datetime import datetime

from fastapi import BackgroundTasks, FastAPI
from fastapi_health import health
from fastapi_utils.tasks import repeat_every
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, MetaData, select, String, Table
from sqlalchemy.sql import text

from app.bulletin_scraper import get_a_month
from app.bulletin_search import copy_db


app = FastAPI()
btasks = BackgroundTasks()
engine = create_engine(
    "sqlite:///bulletin_search.db", pool_recycle=10000, echo=False, future=True
)

# TODO: put Table into a separate file and import it here and bulletin_scraper
metadata = MetaData(bind=engine)
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


class ScrapeDate(BaseModel):
    day: int
    month: int
    year: int


class Query(BaseModel):
    query_string: str


@app.get("/")
async def root():
    """The root enpoint has not been implemented."""
    return {"message": "there is nothing here, please post a msg to /scrape_article"}


@app.post("/search_by_issue_date")
def search_by_issue_date(scrapeDate: ScrapeDate):
    """Search bulletins by issue date.
    Returns every item issued on the given date"""
    conn = engine.connect()
    date = "/".join(
        [
            str(scrapeDate.year),
            str(scrapeDate.month).zfill(2),
            str(scrapeDate.day).zfill(2),
        ]
    )
    s = select([bulletins]).where(bulletins.c.doc_date == date)
    rp = conn.execute(s)
    records = rp.fetchall()
    conn.close()
    return {"query": date, "results": list(records)}


@app.post("/search_by_scrape_date")
def search_by_scrape_date(scrapeDate: ScrapeDate):
    """Search bulletins by scrape date.
    Returns every item scraped on the given date."""
    conn = engine.connect()
    date = "/".join(
        [
            str(scrapeDate.year),
            str(scrapeDate.month).zfill(2),
            str(scrapeDate.day).zfill(2),
        ]
    )
    s = select([bulletins]).where(bulletins.c.scrape_date == date)
    rp = conn.execute(s)
    records = rp.fetchall()
    conn.close()
    return {"query": date, "results": list(records)}


@app.post("/search_by_term")
def search_by_term(query: Query):
    """Search the full text field of the bulletins.
    Use simple matching and Boolean queries.
    WARNING: not for production, it can be used for sql injection attack!!!!
    """
    search_string = text(query.query_string)
    conn = engine.connect()
    rp = conn.execute(search_string)
    records = rp.fetchall()
    conn.close()
    return {"query": query.query_string, "results": records}


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
    """Runs bulletin_scraper.get_a_month() once a day
    Copies the bulletin.db into bulletin_search.db
    """
    print("Hello-bello" * 30)
    month = datetime.now().month
    year = datetime.now().year
    get_a_month(month, year)
    copy_db()
    return None


##### Health check
def pass_condition():
    # TODO: elaborate this function
    return {"autotagging": "online"}


def sick_condition():
    # TODO: add sick condition
    return False


app.add_api_route("/health", health([pass_condition, sick_condition]))
