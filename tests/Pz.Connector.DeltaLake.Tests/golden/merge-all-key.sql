MERGE INTO target USING source ON target."id" = source."id" AND target."dt" = source."dt"
WHEN NOT MATCHED THEN INSERT ("id", "dt") VALUES (source."id", source."dt")
