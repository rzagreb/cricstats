import os
from typing import Any

from app.infrastructure.database.db_wrapper import PostgresWrapper

CUR_DIR = os.path.dirname(os.path.realpath(__file__))
SQL_DIR = os.path.join(CUR_DIR, "sql")

REPORT_MODES = {
    "top_batsmen": """
        SELECT 
            p.name AS batter_name,
            SUM(od.runs_batter) AS total_runs
        FROM overs_deliveries od
        INNER JOIN people p ON od.batter_id = p.person_id
        INNER JOIN matches m ON od.match_id = m.match_id
        WHERE m.season = %s
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10;
    """,
    "top_batter_strike_rates": """
        SELECT 
            p.name AS batter_name,
            SUM(od.runs_batter) AS total_runs,
            COUNT(od.delivery_id) AS balls_faced,
            ROUND((SUM(od.runs_batter)::decimal / COUNT(od.delivery_id)) * 100, 2) AS strike_rate
        FROM overs_deliveries od
        INNER JOIN people p ON od.batter_id = p.person_id
        INNER JOIN matches m ON od.match_id = m.match_id
        WHERE m.season = %s
        GROUP BY 1
        ORDER BY strike_rate DESC
        LIMIT 10;
    """,
    "top_wicket_takers": """
        SELECT 
            p.name AS bowler_name,
            COUNT(*) AS total_wickets
        FROM overs_deliveries od
        INNER JOIN people p ON od.bowler_id = p.person_id
        INNER JOIN matches m ON od.match_id = m.match_id
        WHERE
            m.season = %s
            AND od.wickets IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10;
    """,
}


class QueryService:
    def get_season_report(self, report: str, season: str) -> list[dict[str, Any]]:
        """Generate a report."""
        if report not in REPORT_MODES:
            raise ValueError(
                f"Invalid report query: {report}. Available queries: {REPORT_MODES.keys()}"
            )

        sql = REPORT_MODES[report]

        db = PostgresWrapper()
        result = db.execute(sql, (season,))
        return result
