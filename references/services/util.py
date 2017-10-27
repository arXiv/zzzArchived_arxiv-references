from flask import g, Flask
from flask import current_app as flask_app
#from celery import current_app as celery_app
#from celery._state import default_app as default_celery_app

import os
import subprocess
import shlex
import shutil
import tempfile
from contextlib import contextmanager
from references.types import List
from references import logging

logger = logging.getLogger(__name__)

VolumeList = List[List[str]]
PortList = List[List[int]]


def run_docker(image: str, volumes: VolumeList = [], ports: PortList = [],
               args: str = '', daemon: bool = False) -> (str, str):
    """
    Run a generic docker image. In our uses, we wish to set the userid to that
    of running process (getuid) by default. Additionally, we do not expose
    any ports for running services making this a rather simple function.

    Parameters
    ----------
    image : str
        Name of the docker image in the format 'repository/name:tag'

    volumes : list of tuple of str
        List of volumes to mount in the format [host_dir, container_dir].

    args : str
        Arguments to the image's run cmd (set by Dockerfile CMD)

    daemon : boolean
        If True, launches the task to be run forever
    """
    # we are only running strings formatted by us, so let's build the command
    # then split it so that it can be run by subprocess
    opt_user = '-u {}'.format(os.getuid())
    opt_volumes = ' '.join(['-v {}:{}'.format(hd, cd) for hd, cd in volumes])
    opt_ports = ' '.join(['-p {}:{}'.format(hp, cp) for hp, cp in ports])
    cmd = 'docker run --rm {} {} {} {} {}'.format(
        opt_user, opt_ports, opt_volumes, image, args
    )
    cmd = shlex.split(cmd)

    if daemon:
        return subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if result.returncode:
        logger.error(
            "Docker image call '{}' returned error {}".format(
                ' '.join(cmd), result.returncode
            )
        )
        logger.error(
            "STDOUT: {}\nSTDERR: {}".format(result.stdout, result.stderr)
        )
        result.check_returncode()

    return result


@contextmanager
def tempdir(cleanup: bool = True) -> str:
    """
    A near copy of tempfile.TemporaryDirectory but does not clean up
    automatically after calling. Useful for debugging purposes for troublesome
    pdfs in the workflow.

    Parameters
    ----------
    cleanup: bool
        Whether to delete the directory after use

    Returns
    -------
    directory: str
        Temporary directory name
    """
    directory = tempfile.mkdtemp()
    try:
        yield directory
    except Exception as e:
        raise e
    finally:
        if cleanup:
            shutil.rmtree(directory)


def get_application_config(app: object = None) -> dict:
    """
    Get a configuration from the current app, or fall back to env.

    Parameters
    ----------
    app : :class:`flask.Flask` or :class:`celery.Celery`

    Returns
    -------
    dict-like
        This is either the current application configuration (from Flask or
        Celery), or ``os.environ``. Any of these should support the ``get()``
        method.
    """
    try:
        import celery
        import celery._state
    except ImportError:
        celery = None
    if app is not None:
        if isinstance(app, Flask):
            return app.config
        if celery is not None and isinstance(app, celery.Celery):
            return app.conf
    if flask_app:    # Proxy object; falsey if there is no application context.
        return flask_app.config
    # If no application context is available, current_app will proxy a new
    # Celery application, the default_app. Since it's a Celery, too, the only
    # way that I can see to detect this case is to directly compare it to the
    # object proxied by current_app.
    if celery is not None and celery._state.current_app and \
            celery._state.current_app._get_current_object() \
            is not celery._state.default_app:
        return celery.current_app.conf
    return os.environ


def get_application_global() -> object:
    """
    Get the current application global proxy object.

    Returns
    -------
    proxy or None
    """
    if g:
        return g
    return
