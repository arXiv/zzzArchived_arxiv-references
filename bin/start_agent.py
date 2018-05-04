"""Run the reference extraction agent."""

from arxiv.base.agent import process_stream
from references.factory import create_web_app
from references.agent import ExtractionAgent


def start_agent() -> None:
    """Start the record processor."""
    app = create_web_app()
    with app.app_context():
        process_stream(ExtractionAgent, app.config)


if __name__ == '__main__':
    start_agent()
