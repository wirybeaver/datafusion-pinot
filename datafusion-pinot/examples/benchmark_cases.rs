//! Benchmark case definitions

pub struct BenchmarkCase {
    pub name: &'static str,
    pub description: &'static str,
    pub sql: &'static str,
}

pub const BENCHMARK_CASES: &[BenchmarkCase] = &[
    BenchmarkCase {
        name: "1",
        description: "COUNT(*) query",
        sql: r#"SELECT COUNT(*) FROM pinot.default."baseballStats""#,
    },
    BenchmarkCase {
        name: "2",
        description: "Full scan with dictionary-encoded columns",
        sql: r#"SELECT "playerName", "hits", "homeRuns" FROM pinot.default."baseballStats""#,
    },
    BenchmarkCase {
        name: "3",
        description: "Full scan with RAW-encoded column",
        sql: r#"SELECT "playerID", "hits", "homeRuns" FROM pinot.default."baseballStats""#,
    },
    BenchmarkCase {
        name: "4",
        description: "Simple aggregation (SUM, AVG, MAX)",
        sql: r#"SELECT SUM("hits") as total_hits, AVG("homeRuns") as avg_homeruns, MAX("strikeouts") as max_strikeouts FROM pinot.default."baseballStats""#,
    },
    BenchmarkCase {
        name: "5",
        description: "GROUP BY with aggregation and ORDER BY",
        sql: r#"SELECT "teamID", COUNT(*) as games, SUM("hits") as total_hits FROM pinot.default."baseballStats" GROUP BY "teamID" ORDER BY total_hits DESC LIMIT 10"#,
    },
    BenchmarkCase {
        name: "6",
        description: "GROUP BY with RAW-encoded column",
        sql: r#"SELECT "playerID", AVG("homeRuns") as avg_home_runs FROM pinot.default."baseballStats" GROUP BY "playerID" ORDER BY avg_home_runs DESC LIMIT 10"#,
    },
    BenchmarkCase {
        name: "7",
        description: "Projection with LIMIT 10",
        sql: r#"SELECT "playerID", "hits", "homeRuns" FROM pinot.default."baseballStats" LIMIT 10"#,
    },
];
