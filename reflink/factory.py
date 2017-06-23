import sys, os

from reflink import config
from reflink.views import rest

from invenio_config import create_config_loader
from invenio_base.app import create_app_factory
from invenio_base.wsgi import create_wsgi_factory, wsgi_proxyfix
from flask import Flask

app_class = Flask    # Invenio sub-classes this for some applications.

env_prefix = ''
instance_path = os.environ.get(env_prefix + '_INSTANCE_PATH',
                               os.path.join(sys.prefix, 'var', 'instance'))

config_loader = create_config_loader(config=config)


# Invenio base is pretty broken at the moment....
#
# create_api = create_app_factory(
#     'reflink',
#     config_loader=config_loader,
#     blueprint_entry_points=['invenio_base.api_blueprints'],
#     extension_entry_points=['invenio_base.api_apps'],
#     converter_entry_points=['invenio_base.api_converters'],
#     wsgi_factory=wsgi_proxyfix(),
#     instance_path=instance_path,
#     app_class=app_class('reflink')
# )
#
# create_app = create_app_factory(
#     'reflink',
#     config_loader=config_loader,
#     blueprint_entry_points=['invenio_base.blueprints'],
#     extension_entry_points=['invenio_base.apps'],
#     converter_entry_points=['invenio_base.converters'],
#     wsgi_factory=wsgi_proxyfix(create_wsgi_factory({'/api': create_api})),
#     instance_path=instance_path
# )


def create_app():
    app = Flask('reflink')
    app.config.from_pyfile('config.py')
    app.register_blueprint(rest.blueprint)
    return app
