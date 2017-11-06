"""Execute the KCL consumer process."""

from amazon_kclpy import kcl
from references.agent.consumer import RecordProcessor
from references.factory import create_web_app


if __name__ == "__main__":
    app = create_web_app()
    app.app_context().push()
    kcl_process = kcl.KCLProcess(RecordProcessor())
    kcl_process.run()
