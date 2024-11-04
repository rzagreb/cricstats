-- People
-------------------------------------------------
CREATE TABLE IF NOT EXISTS people (
    person_id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_people_name ON people(name);

-- Teams
-------------------------------------------------
CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    team_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_teams_name ON teams(name);

-- Matches
-------------------------------------------------
-- https://cricsheet.org/format/json/#outcome
CREATE TABLE IF NOT EXISTS matches (
    match_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    match_number INTEGER,
    match_type TEXT,
    season TEXT,
    gender TEXT,
    outcome_by JSONB,
    outcome_bowl_out_team_id INTEGER REFERENCES teams(team_id),
    outcome_eliminator_team_id INTEGER REFERENCES teams(team_id),
    outcome_method TEXT,
    outcome_result TEXT, -- draw, no result, or tie.
    outcome_winner_team_id INTEGER REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_matches_name ON matches(name);


-- Match Teams
-------------------------------------------------
CREATE TABLE IF NOT EXISTS match_teams (
    match_id INTEGER REFERENCES matches(match_id),
    team_id INTEGER REFERENCES teams(team_id),
    PRIMARY KEY (match_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_match_teams_team_id ON match_teams(team_id);

-- Match Players
-------------------------------------------------
CREATE TABLE IF NOT EXISTS match_players (
    match_players_id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES matches(match_id),
    team_id INTEGER REFERENCES teams(team_id),
    player_id TEXT REFERENCES people(person_id)
);

CREATE INDEX IF NOT EXISTS idx_match_players_player_id ON match_players(player_id);

CREATE INDEX IF NOT EXISTS idx_match_players_team_id ON match_players(team_id);


-- overs->deliveries
-------------------------------------------------
CREATE TABLE IF NOT EXISTS overs_deliveries (
    delivery_id SERIAL PRIMARY KEY,

    match_id INTEGER REFERENCES matches(match_id),
    team_id INTEGER REFERENCES teams(team_id),

    innings_number INTEGER NOT NULL,
    over_number INTEGER NOT NULL,
    delivery_number INTEGER NOT NULL,

    batter_id TEXT REFERENCES people(person_id),
    bowler_id TEXT REFERENCES people(person_id),
    non_striker_id TEXT REFERENCES people(person_id),

    runs_batter INTEGER,
    runs_extras INTEGER,
    runs_total INTEGER,
    runs_non_boundary BOOLEAN NULL,

    extras_byes INTEGER NULL,
    extras_legbyes INTEGER NULL,
    extras_noballs INTEGER NULL,
    extras_penalty INTEGER NULL,
    extras_wides INTEGER NULL,
    replacements JSONB NULL,
    review JSONB NULL,
    wickets JSONB NULL
);
