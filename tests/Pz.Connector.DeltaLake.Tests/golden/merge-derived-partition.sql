MERGE INTO target USING source ON target."id" = source."id" AND target."dt" = source."dt" AND target."dt" IN ('2026-01-01', '2026-01-02')
WHEN MATCHED THEN UPDATE SET target."amt" = source."amt"
WHEN NOT MATCHED THEN INSERT ("id", "dt", "amt") VALUES (source."id", source."dt", source."amt")
