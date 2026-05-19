import click
import datetime as dt
from taq_etl.dal.models.taq_file import TaqType
from taq_etl.service.raw_taq_service import RawTaqService

@click.group(name="utils")
@click.pass_context
def utils_group(ctx):
    """Utility commands for inspecting TAQ files and database state."""
    pass

@utils_group.command(name="print-size")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def print_size(obj, date: dt.datetime, type: str):
    """Detect and print the record size for a specific daily binary file."""
    service: RawTaqService = obj['raw_taq']
    service.print_record_size_for_day(date.date(), TaqType(type))

@utils_group.command(name="print-bin-hexdump")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def print_bin_hexdump(obj, date: dt.datetime, type: str):
    """Print the hexdump of the .BIN binary file for a specific date and type."""
    service: RawTaqService = obj['raw_taq']
    service.print_rawbin_for_day(date.date(), TaqType(type))

@utils_group.command(name="print-index-hexdump")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def print_index_hexdump(obj, date: dt.datetime, type: str):
    """Print the hexdump of the .IDX index file for a specific date and type."""
    service: RawTaqService = obj['raw_taq']
    service.print_rawidx_for_day(date.date(), TaqType(type))

@utils_group.command(name="print-index-df")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def print_index(obj, date: dt.datetime, type: str):
    """Load and print the index DataFrame for a specific date and type."""
    service: RawTaqService = obj['raw_taq']
    service.print_index_for_day(date.date(), TaqType(type))

@utils_group.command(name="print-idx-by-date")
@click.option("--date", "-d", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--type", "-t", type=click.Choice(["CT", "CQ"]), required=True)
@click.pass_obj
def print_idx_by_date(obj, date: dt.datetime, type: str):
    """Group the index by date and show per-date summary (ticker count, record range, total records)."""
    service: RawTaqService = obj['raw_taq']
    service.print_idx_by_date(date.date(), TaqType(type))