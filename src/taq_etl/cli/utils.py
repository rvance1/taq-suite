import click
import datetime as dt
from taq_etl.dal.models.taq_file import TaqType
from taq_etl.service.raw_taq_service import RawTaqService

@click.group(name="utils")
def utils_group():
    """Utility commands for inspecting TAQ files and database state."""
    pass

@utils_group.command(name="print-size")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def print_size(service: RawTaqService, date: dt.datetime, type: str):
    """Detect and print the record size for a specific daily binary file."""
    service.print_record_size_for_day(date.date(), TaqType(type))

@utils_group.command(name="print-hexdump")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def print_hexdump(service: RawTaqService, date: dt.datetime, type: str):
    """Print the hexdump of the binary file for a specific date and type."""
    service.print_hexdump_for_day(date.date(), TaqType(type))

@utils_group.command(name="print-index")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def print_index(service: RawTaqService, date: dt.datetime, type: str):
    """Load and print the index DataFrame for a specific date and type."""
    service.print_index_for_day(date.date(), TaqType(type))