import logging

from app.infrastructure.data_ingestion.ingestor import DataIngestor
from app.infrastructure.database.db_wrapper import PostgresWrapper

INGEST_MODES = {
    "odi_all": {"url": "https://cricsheet.org/downloads/odis_json.zip"},
    # "2019": "https://cricsheet.org/downloads/2019_json.zip",
}
log = logging.getLogger(__name__)


class IngestService:
    def run_ingestion(self, ingest_mode: str):
        """Run the data ingestion process."""
        if ingest_mode not in INGEST_MODES:
            raise ValueError(
                f"Invalid ingest mode: {ingest_mode}. Available modes: {INGEST_MODES.keys()}"
            )

        db = PostgresWrapper()
        ingestor = DataIngestor()

        url = INGEST_MODES[ingest_mode]["url"]

        filepath_zip = ingestor.download(url=url)
        dirpath_unzipped = ingestor.process_zip(filepath_zip=filepath_zip)
        ingestor.ingest(dirpath_unzipped, db_wrapper=db)
