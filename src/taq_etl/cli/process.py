import click
import datetime as dt
from taq_etl.dal.models.taq_file import TaqType
from taq_etl.service.raw_taq_service import RawTaqService

@click.group(name="process")
@click.pass_context
def process_group(ctx):
    """Commands for processing raw TAQ data into Parquet."""
    pass

@process_group.command(name="day")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def process_day(obj, date: dt.datetime, type: str):
    """Process a single day of TAQ data."""
    raw_taq_service: RawTaqService = obj['raw_taq']
    raw_taq_service.process_for_day(date.date(), TaqType(type))

@process_group.command(name="month")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m"]), required=True, help="Month to process in YYYY-MM format")
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def process_month(obj, date: dt.datetime, type: str):
    """Process an entire month of TAQ data iteratively."""
    raw_taq_service: RawTaqService = obj['raw_taq']
    raw_taq_service.process_for_month(date.year, date.month, TaqType(type))

@process_group.command(name="range")
@click.option("--start", "-s", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--end", "-e", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def process_range(obj, start: dt.datetime, end: dt.datetime, type: str):
    """Process a date range in parallel."""
    raw_taq_service: RawTaqService = obj['raw_taq']
    raw_taq_service.process_range_parallel(start.date(), end.date(), TaqType(type))