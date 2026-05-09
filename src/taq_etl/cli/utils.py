import click
import datetime as dt
from taq_etl.dal.models.taq_file import TaqType
from taq_etl.dal.models.database import Database
from taq_etl.service.raw_taq_service import RawTaqService

@click.group(name="utils")
def utils_group():
    """Utility commands for inspecting TAQ files and database state."""
    pass

@utils_group.command(name="print-size")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["trades", "quotes"]), required=True)
def print_size(date: dt.datetime, type: str):
    """Detect and print the record size for a specific daily binary file."""
    db = Database()
    service = RawTaqService(database=db)
    service.print_record_size_for_day(date.date(), TaqType(type))