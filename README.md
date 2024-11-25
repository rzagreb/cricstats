# README

If you’re someone who loves digging into the details of cricket and finding cool patterns in the data, this project is right up your alley. It takes data from cricsheet.org (an awesome resource for cricket stats) and turns it into detailed reports about matches, players, and teams. Whether you’re here to geek out over cricket stats or just explore some interesting data, this guide has you covered—from getting set up to running your first report.

## Prerequisites

The following guideline works only for the MacOS.

1. Install package manager [Homebrew](https://brew.sh/).
2. Install command `brew install make`.
3. Install Docker using `brew install --cask docker`.
4. Install Python >=3.9 (MacOs)
   1. Install python version manager `brew install pyenv`
   2. Install specific python version `pyenv install 3.9.11`
   3. Set this version as global `pyenv global 3.9.2`

## Setup Instructions

1. Set environment variables in `.env` file (or export them in the shell):
  - `POSTGRES_HOST`: Postgres host. (e.g., `localhost`)
  - `POSTGRES_PORT`: Postgres port. (e.g., `5432`)
  - `POSTGRES_DB`: Postgres database name. (e.g., `cricket`)
  - `POSTGRES_USER`: Postgres user. (e.g., `postgres`)
  - `POSTGRES_PASSWORD`: Postgres password. (e.g., `password`)
- Run setup to create a Python virtual environment, install dependencies, and start the PostgreSQL instance.
   ```bash
   make setup
   ```

## Data Ingestion

Run the data ingestion process to download and load ODI match data into the database. Should take about 20 min.

```bash
make ingest_odi_all
```

Data will be downloaded, normalized, and saved in the following tables:

- `match_players`: players participating in each match, linking players with their respective teams in specific matches. 
- `match_teams`: Represents the teams participating in each match, creating a link between matches and teams.
- `matches`: matches their properties and outcomes.
- `overs_deliveries`: Stores detailed information about each delivery in every match, capturing events like runs, extras, wickets, and specific delivery actions.
- `people`: players names and associated ids
- `teams`: teams names and team_types

## Running Reports

To run the reports, use the following command:

```bash
python run.py report --name <REPORT_NAME> --season <YEAR>
```
For example:

```bash
python run.py report --name top10_batsmen --season 2023
# batter_name        | total_runs
# -------------------+-----------
# SR Taylor          | 706       
# SC Williams        | 625       
# Rahmanullah Gurbaz | 616       
# V Aravind          | 569       
# Waseem Muhammad    | 555       
# HT Tector          | 527       
# P Nissanka         | 516       
# Ibrahim Zadran     | 511       
# PR Stirling        | 479       
# G Singh            | 464       
```

Available reports:

- `top_batsmen`: Top 10 batsmen by total runs in a season.
- `top_batter_strike_rates`: Top 10 batsmen by strike rate in a season.
- `top_wicket_takers`: Top 10 bowlers by total wickets in a season.
