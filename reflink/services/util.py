import os
import subprocess
import shlex
import shutil
import tempfile
from contextlib import contextmanager
from reflink.types import List

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
