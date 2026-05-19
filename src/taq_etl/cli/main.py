import click
from taq_etl.config import settings
from taq_etl.dal.models.database import Database
from taq_etl.service.raw_taq_service import RawTaqService
from taq_etl.service.crsp_service import CrspService

from taq_etl.cli.process import process_group
from taq_etl.cli.build_map import build_map_group
from taq_etl.cli.utils import utils_group

@click.group()
@click.pass_context
def cli(ctx):
    """TAQ ETL: High-performance financial data processing."""
    db = Database(raw_taq_path=settings.raw_taq_path, output_path=settings.output_path)
    ctx.obj = {
        "raw_taq": RawTaqService(database=db),
        "crsp": CrspService(database=db)
    }

cli.add_command(process_group)
cli.add_command(utils_group)
cli.add_command(build_map_group)

if __name__ == "__main__":
    cli()