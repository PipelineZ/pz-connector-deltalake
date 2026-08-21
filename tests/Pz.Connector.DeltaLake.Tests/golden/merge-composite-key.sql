MERGE INTO target USING source ON target."id" = source."id" AND target."dt" = source."dt"
WHEN MATCHED THEN UPDATE SET target."amt" = source."amt"
WHEN NOT MATCHED THEN INSERT ("id", "dt", "amt") VALUES (source."id", source."dt", source."amt")
