"""Execute the KCL consumer process."""

import warnings
from amazon_kclpy import kcl
from references.agent.consumer import RecordProcessor
from references.factory import create_web_app


if __name__ == "__main__":
    app = create_web_app()
    app.app_context().push()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kcl_process = kcl.KCLProcess(RecordProcessor())
        kcl_process.run()
