from datetime import datetime

from fastapi import BackgroundTasks, FastAPI
from fastapi_health import health
from fastapi_utils.tasks import repeat_every
from pydantic import BaseModel, HttpUrl

from app.bulletin_scraper import get_a_month

app = FastAPI()
btasks = BackgroundTasks()


@app.get("/")
async def root():
    """The root enpoint has not been implemented."""
    return {"message": "there is nothing here, please post a msg to /scrape_article"}


@app.post("/search_by_issue_date")
def search_by_issue_date():
    """Search bulletins by issue date.
    Returns every item issued on the given date"""
    pass


@app.post("/search_by_scrape_date")
def search_by_scrape_date():
    """Search bulletins by scrape date.
    Returns every item scraped on the given date."""
    pass


@app.post("/search_by_term")
def search_by_term():
    """Search the full text field of the bulletins.
    Use simple matching and Boolean queries."""
    pass


@app.get("/scrape_now")
def scrape_now(month, year):
    """Returns only acknowledgement since this starts a background job"""
    btasks.add_task(get_a_month, month, year)
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
