import json
import logging
import os
import shutil
import zipfile

import requests

from app.application.consts import DATA_RAW_DIR, DATA_UNZIPPED_DIR
from app.infrastructure.database.db_wrapper import PostgresWrapper
from app.infrastructure.database.models import NormRef

log = logging.getLogger(__name__)
DbSchemaName = "public"
DirectoryPathType = str
FilePathType = str


class DataIngestor:
    def __init__(self):
        self.dir_raw = DATA_RAW_DIR
        self.dir_unzipped = DATA_UNZIPPED_DIR

        os.makedirs(self.dir_raw, exist_ok=True)
        os.makedirs(self.dir_unzipped, exist_ok=True)

    def download(self, url: str) -> FilePathType:
        """Download the data from the url

        Args:
            url (str): The url from which to download the data which is a zipped json file

        Returns:
            str: The path to the downloaded file
        """
        log.info(f"Downloading: {url}")

        filename = url.split("/")[-1]
        filepath = os.path.join(self.dir_raw, filename)

        # delete the file if it already exists
        if os.path.exists(filepath):
            os.remove(filepath)

        try:
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                chunk_size = 1024 * 1024  # Chunk size: 1MB

                with open(filepath, "wb") as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        # Filter out keep-alive chunks
                        if chunk:
                            file.write(chunk)
            log.info(f"Downloaded {filename}.")

        except requests.exceptions.HTTPError as http_err:
            log.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            log.error(f"An error occurred: {err}")

        log.info(f"Saved {self.dir_raw}")

        return filepath

    def process_zip(self, filepath_zip: str) -> DirectoryPathType:
        """Process the zipped file and extract the contents

        Args:
            filepath_zip (str): The path to the zipped file

        Returns:
            str: The path to the unzipped directory
        """
        log.info(f"Processing: {filepath_zip}")

        filename = os.path.basename(filepath_zip)
        filename_no_ext = os.path.splitext(filename)[0]

        output_dir = os.path.join(self.dir_unzipped, filename_no_ext)

        # delete the directory if it already exists
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

        try:
            with zipfile.ZipFile(filepath_zip, "r") as zip_ref:
                zip_ref.extractall(output_dir)
            log.info(f"Extracted '{filepath_zip}' to '{output_dir}'")
        except zipfile.BadZipFile:
            log.error(f"Error: The file '{filepath_zip}' is a corrupt zip file.")

        return output_dir

    def ingest(self, dir_with_json: DirectoryPathType, db_wrapper: PostgresWrapper):
        """Ingest the data from the json files into the database

        Args:
            dir_with_json (str): The directory containing the json files
            db_wrapper (PostgresWrapper): The database wrapper

        Returns:
            None
        """
        filepaths = [
            os.path.join(dir_with_json, filename)
            for filename in os.listdir(dir_with_json)
            if filename.endswith(".json")
        ]
        files_cnt = len(filepaths)
        for i, filepath in enumerate(filepaths):
            log.info(f"{i}/{files_cnt}. Ingesting: {filepath}")
            self._ingest_file(filepath, db_wrapper)
        log.info(f"Ingestion complete ({len(filepaths)} files).")

    def _ingest_file(self, filepath: FilePathType, db_wrapper: PostgresWrapper):
        with open(filepath, "r") as file:
            data = json.load(file)
        self._ingest_values(data, db_wrapper)

    def _ingest_values(self, data, db: PostgresWrapper):
        """Insert new Ref values into the database if they do not already exist"""
        log.info("Ingesting values...")

        info = data["info"]

        match_name = (
            info.get("event", {}).get("name")
            or f"{info['season']}|{info['match_type']}|{info['gender']}|{info['venue']}|{info.get('city')}|"
        )
        match_number = info.get("event", {}).get("match_number", -1)

        # Some ids are missing for people so we skip it for now
        people_map = [
            {"person_id": _id, "name": name}
            for name, _id in info["registry"].get("people", {}).items()
        ]
        for team_players in info.get("players", {}).values():
            for player_name in team_players:
                if player_name not in info["registry"]["people"]:
                    people_map.append({"person_id": player_name, "name": player_name})

        db.insert_values(
            table="people",
            rows=people_map,
            unique_columns=["person_id"],
        )

        db.insert_values(
            table="teams",
            rows=[
                {"name": team_name, "team_type": info["team_type"]}
                for team_name in info["teams"]
            ],
            unique_columns=["name"],
        )

        db.insert_values(
            table="matches",
            rows=[
                {
                    # to insert
                    "name": match_name,
                    "match_number": match_number,
                    "match_type": info["match_type"],
                    "season": info["season"],
                    "gender": info["gender"],
                    "outcome_by": (
                        info["outcome"].get("by") if info["outcome"].get("by") else None
                    ),
                    "outcome_bowl_out_team_id": None,
                    "outcome_eliminator_team_id": None,
                    "outcome_method": info["outcome"].get("method"),
                    "outcome_result": info["outcome"].get("result"),
                    "outcome_winner_team_id": None,
                    # to match
                    "outcome_eliminator_team_name": info["outcome"].get("eliminator"),
                    "outcome_bowl_out_team_name": info["outcome"].get("bowl_out"),
                    "outcome_winner_team_name": info["outcome"].get("winner"),
                }
            ],
            columns_to_insert=[
                "name",
                "match_number",
                "match_type",
                "season",
                "gender",
                "outcome_by",
                "outcome_bowl_out_team_id",
                "outcome_eliminator_team_id",
                "outcome_method",
                "outcome_result",
                "outcome_winner_team_id",
            ],
            unique_columns=[
                ("name", "match_number"),
            ],
            norm_values={
                "outcome_bowl_out_team_id": NormRef(
                    t2_key_value="team_id",
                    t2_name="teams",
                    t1_key_join="outcome_bowl_out_team_name",
                    t2_key_join="name",
                ),
                "outcome_eliminator_team_id": NormRef(
                    t2_key_value="team_id",
                    t2_name="teams",
                    t1_key_join="outcome_eliminator_team_name",
                    t2_key_join="name",
                ),
                "outcome_winner_team_id": NormRef(
                    t2_key_value="team_id",
                    t2_name="teams",
                    t1_key_join="outcome_winner_team_name",
                    t2_key_join="name",
                ),
            },
            custom_columns_types={
                "outcome_by": "JSONB",
            },
        )

        db.insert_values(
            table="match_teams",
            rows=[
                {
                    # Column to insert
                    "match_id": None,
                    "team_id": None,
                    # Columns below are only for matching
                    "name": match_name,
                    "match_number": match_number,
                    "team_name": team_name,
                }
                for team_name in info["teams"]
            ],
            columns_to_insert=["match_id", "team_id"],
            unique_columns=[("match_id", "team_id")],
            norm_values={
                "match_id": NormRef(
                    t2_key_value="match_id",
                    t2_name="matches",
                    t1_key_join=("name", "match_number"),
                    t2_key_join=("name", "match_number"),
                ),
                "team_id": NormRef(
                    t2_key_value="team_id",
                    t2_name="teams",
                    t1_key_join="team_name",
                    t2_key_join="name",
                ),
            },
        )

        db.insert_values(
            table="match_players",
            rows=[
                {
                    # Column to insert
                    "match_id": None,
                    "team_id": None,
                    "player_id": None,
                    # Columns below are only for matching
                    "name": match_name,
                    "match_number": match_number,
                    "team_name": team,
                    "player_name": player_name,
                }
                for team, players in info.get("players", {}).items()
                for player_name in players
            ],
            columns_to_insert=["match_id", "team_id", "player_id"],
            unique_columns=[("match_id", "team_id", "player_id")],
            norm_values={
                "match_id": NormRef(
                    t2_key_value="match_id",
                    t2_name="matches",
                    t1_key_join=("name", "match_number"),
                    t2_key_join=("name", "match_number"),
                ),
                "team_id": NormRef(
                    t2_key_value="team_id",
                    t2_name="teams",
                    t1_key_join="team_name",
                    t2_key_join="name",
                ),
                "player_id": NormRef(
                    t2_key_value="person_id",
                    t2_name="people",
                    t1_key_join="player_name",
                    t2_key_join="name",
                ),
            },
        )

        db.insert_values(
            table="overs_deliveries",
            rows=[
                {
                    # to insert
                    "match_id": None,
                    "team_id": None,
                    "innings_number": innings_n,
                    "over_number": over["over"],
                    "delivery_number": delivery_n,
                    "batter_id": None,
                    "bowler_id": None,
                    "non_striker_id": None,
                    "runs_batter": delivery["runs"]["batter"],
                    "runs_extras": delivery["runs"]["extras"],
                    "runs_total": delivery["runs"]["total"],
                    "runs_non_boundary": delivery["runs"].get("non_boundary"),
                    "extras_byes": delivery.get("extras", {}).get("byes"),
                    "extras_legbyes": delivery.get("extras", {}).get("legbyes"),
                    "extras_noballs": delivery.get("extras", {}).get("noballs"),
                    "extras_penalty": delivery.get("extras", {}).get("penalty"),
                    "extras_wides": delivery.get("extras", {}).get("wides"),
                    "replacements": (
                        delivery.get("replacements")
                        if delivery.get("replacements")
                        else None
                    ),
                    "review": (
                        delivery.get("review") if delivery.get("review") else None
                    ),
                    "wickets": (
                        delivery.get("wickets") if delivery.get("wickets") else None
                    ),
                    # to match `match_id`
                    "match_name": match_name,
                    "match_number": match_number,
                    # to match `team_id`
                    "team_name": innings_elem["team"],
                    # to match other columns
                    "batter_name": delivery["batter"],
                    "bowler_name": delivery["bowler"],
                    "non_striker_name": delivery["non_striker"],
                }
                for innings_n, innings_elem in enumerate(data["innings"])
                for over in innings_elem["overs"]
                for delivery_n, delivery in enumerate(over["deliveries"])
            ],
            columns_to_insert=[
                "match_id",
                "team_id",
                "innings_number",
                "over_number",
                "delivery_number",
                "batter_id",
                "bowler_id",
                "non_striker_id",
                "runs_batter",
                "runs_extras",
                "runs_total",
                "runs_non_boundary",
                "extras_byes",
                "extras_legbyes",
                "extras_noballs",
                "extras_penalty",
                "extras_wides",
                "replacements",
                "review",
                "wickets",
            ],
            unique_columns=[
                (
                    "match_id",
                    "team_id",
                    "innings_number",
                    "over_number",
                    "delivery_number",
                ),
            ],
            norm_values={
                "match_id": NormRef(
                    t2_key_value="match_id",
                    t2_name="matches",
                    t1_key_join=("match_name", "match_number"),
                    t2_key_join=("name", "match_number"),
                ),
                "team_id": NormRef(
                    t2_key_value="team_id",
                    t2_name="teams",
                    t1_key_join="team_name",
                    t2_key_join="name",
                ),
                "batter_id": NormRef(
                    t2_key_value="person_id",
                    t2_name="people",
                    t1_key_join="batter_name",
                    t2_key_join="name",
                ),
                "bowler_id": NormRef(
                    t2_key_value="person_id",
                    t2_name="people",
                    t1_key_join="bowler_name",
                    t2_key_join="name",
                ),
                "non_striker_id": NormRef(
                    t2_key_value="person_id",
                    t2_name="people",
                    t1_key_join="non_striker_name",
                    t2_key_join="name",
                ),
            },
            custom_columns_types={
                "replacements": "JSONB",
                "review": "JSONB",
                "wickets": "JSONB",
                "runs_non_boundary": "BOOLEAN",
                "extras_byes": "INTEGER",
                "extras_legbyes": "INTEGER",
                "extras_noballs": "INTEGER",
                "extras_penalty": "INTEGER",
                "extras_wides": "INTEGER",
            },
        )
