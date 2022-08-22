import click


def _get_module(version):
    return __import__(f'.migration.{version}', fromlist=['main'])


@click.argument('version')
@click.option('-d', '--debug', help='Enable debug mode')
def migrate(version, debug):
    module = _get_module(version)
    getattr(module, 'main')(debug)
