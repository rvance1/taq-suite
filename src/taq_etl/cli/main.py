import click
from taq_etl.config import settings
from taq_etl.dal.models.database import Database
from taq_etl.service.raw_taq_service import RawTaqService

from taq_etl.cli.process import process_group
from taq_etl.cli.utils import utils_group

@click.group()
@click.pass_context
def cli(ctx):
    """TAQ ETL: High-performance financial data processing."""
    db = Database(root_path=settings.database_path)
    service = RawTaqService(database=db)
    ctx.obj = service

cli.add_command(process_group)
cli.add_command(utils_group)

if __name__ == "__main__":
    cli()