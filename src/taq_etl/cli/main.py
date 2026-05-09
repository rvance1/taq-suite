import click
from taq_etl.cli.process import process_group
from taq_etl.cli.utils import utils_group

@click.group()
def cli():
    """TAQ ETL: High-performance financial data processing."""
    pass

cli.add_command(process_group)
cli.add_command(utils_group)

if __name__ == "__main__":
    cli()