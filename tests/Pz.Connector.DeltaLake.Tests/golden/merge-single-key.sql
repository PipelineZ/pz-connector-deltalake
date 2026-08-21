MERGE INTO target USING source ON target."id" = source."id"
WHEN MATCHED THEN UPDATE SET target."dt" = source."dt", target."amt" = source."amt"
WHEN NOT MATCHED THEN INSERT ("id", "dt", "amt") VALUES (source."id", source."dt", source."amt")
