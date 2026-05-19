import click
from taq_etl.service.crsp_service import CrspService

@click.group(name="build-map")
@click.pass_context
def build_map_group(ctx):
    """Commands for building CRSP mappings."""
    pass

@build_map_group.command(name="year")
@click.option("--year", "-y", type=click.INT, required=True)
@click.pass_obj
def process_year(obj, year: int):
    """Process a single year of CRSP data."""
    crsp_service: CrspService = obj['crsp']
    crsp_service.build_crsp_map_by_year(year)

@build_map_group.command(name="range")
@click.option("--start", "-s", type=click.INT, required=True)
@click.option("--end", "-e", type=click.INT, required=True)
@click.pass_obj
def process_range(obj, start: int, end: int):
    """Process a date range in parallel."""
    crsp_service: CrspService = obj['crsp']
    crsp_service.build_crsp_map_by_range(start, end)