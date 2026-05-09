import click
import datetime as dt
from taq_etl.dal.models.taq_file import TaqType
from taq_etl.dal.models.database import Database
from taq_etl.service.raw_taq_service import RawTaqService

@click.group(name="process")
def process_group():
    """Commands for processing raw TAQ data into Parquet."""
    pass

@process_group.command(name="day")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
def process_day(date: dt.datetime, type: str):
    """Process a single day of TAQ data."""
    db = Database() 
    service = RawTaqService(database=db)
    service.process_for_day(date.date(), TaqType(type))

@process_group.command(name="range")
@click.option("--start", "-s", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--end", "-e", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
def process_range(start: dt.datetime, end: dt.datetime, type: str):
    """Process a date range in parallel."""
    db = Database()
    service = RawTaqService(database=db)
    service.process_range_parallel(start.date(), end.date(), TaqType(type))