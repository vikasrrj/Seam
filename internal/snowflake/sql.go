package snowflake

import (
	"fmt"
	"strings"

	"example.com/seam/internal/schema"
)

func createTargetSQL(cfg Config, source *schema.Schema, table string) (string, error) {
	return createTypedTableSQL(cfg, source, table, "CREATE TABLE IF NOT EXISTS ")
}

func createShadowTableSQL(cfg Config, source *schema.Schema, table string) (string, error) {
	return createTypedTableSQL(cfg, source, table, "CREATE TRANSIENT TABLE ")
}

func createLiveViewSQL(cfg Config, source *schema.Schema, physicalTable string, replace bool) string {
	verb := "CREATE VIEW IF NOT EXISTS "
	if replace {
		verb = "CREATE OR REPLACE VIEW "
	}
	columns := make([]string, len(source.Columns))
	for index, column := range source.Columns {
		columns[index] = quoteIdentifier(column.Name)
	}
	list := strings.Join(columns, ", ")
	return verb + cfg.target(cfg.LiveTable) + " AS SELECT " + list + " FROM " + cfg.target(physicalTable)
}

func createTypedTableSQL(cfg Config, source *schema.Schema, table, prefix string) (string, error) {
	definitions := make([]string, 0, len(source.Columns))
	for _, column := range source.Columns {
		typeName, err := snowflakeType(column)
		if err != nil {
			return "", err
		}
		nullability := ""
		if !column.Nullable {
			nullability = " NOT NULL"
		}
		definitions = append(definitions, quoteIdentifier(column.Name)+" "+typeName+nullability)
	}
	return prefix + cfg.target(table) + " (" + strings.Join(definitions, ", ") + ")", nil
}

func mergeTargetSQL(cfg Config, source *schema.Schema, table string) (string, error) {
	pk := source.PKColumn()
	if pk == nil {
		return "", fmt.Errorf("source schema has no primary key")
	}
	columnNames := make([]string, 0, len(source.Columns))
	values := make([]string, 0, len(source.Columns))
	updates := make([]string, 0, len(source.Columns)-1)
	projection := make([]string, 0, len(source.Columns)+2)
	for _, column := range source.Columns {
		name := quoteIdentifier(column.Name)
		expression, err := valueExpression(column, "R.PAYLOAD")
		if err != nil {
			return "", err
		}
		columnNames = append(columnNames, name)
		values = append(values, "S."+name)
		projection = append(projection, expression+" AS "+name)
		if !column.PrimaryKey {
			updates = append(updates, "D."+name+" = S."+name)
		}
	}
	projection = append(projection, "R.OP", "R.SOURCE_LSN", "R.SEQUENCE AS SOURCE_SEQUENCE")

	// The key clock is intentionally outside the visible target. A DELETE must
	// leave a durable version behind so an older backfill candidate cannot
	// resurrect it. The candidate and CDC paths use this same comparison.
	using := "SELECT " + strings.Join(projection, ", ") +
		" FROM (SELECT S.*, ROW_NUMBER() OVER (PARTITION BY S.PK ORDER BY S.SEQUENCE DESC) AS RN" +
		" FROM " + cfg.internal("CDC_STAGE") + " S WHERE S.BATCH_ID = ?) R" +
		" LEFT JOIN " + cfg.internal("KEY_CLOCKS") + " K ON K.STREAM_ID = ? AND K.ROUTE_ID = ? AND K.PK = R.PK" +
		" WHERE R.RN = 1 AND (K.SOURCE_LSN IS NULL OR R.SOURCE_LSN > K.SOURCE_LSN" +
		" OR (R.SOURCE_LSN = K.SOURCE_LSN AND R.SEQUENCE >= K.SOURCE_SEQUENCE))"

	updateClause := ""
	if len(updates) > 0 {
		updateClause = " WHEN MATCHED AND S.OP <> 'delete' THEN UPDATE SET " + strings.Join(updates, ", ")
	}
	return "MERGE INTO " + cfg.target(table) + " D USING (" + using + ") S ON D." + quoteIdentifier(pk.Name) + " = S." + quoteIdentifier(pk.Name) +
		" WHEN MATCHED AND S.OP = 'delete' THEN DELETE" + updateClause +
		" WHEN NOT MATCHED AND S.OP <> 'delete' THEN INSERT (" + strings.Join(columnNames, ", ") + ") VALUES (" + strings.Join(values, ", ") + ")", nil
}

func mergeClocksSQL(cfg Config) string {
	return "MERGE INTO " + cfg.internal("KEY_CLOCKS") + " K USING (" +
		"SELECT PK, SOURCE_LSN, SEQUENCE AS SOURCE_SEQUENCE, OP = 'delete' AS DELETED FROM (" +
		"SELECT S.*, ROW_NUMBER() OVER (PARTITION BY S.PK ORDER BY S.SEQUENCE DESC) AS RN FROM " + cfg.internal("CDC_STAGE") + " S WHERE S.BATCH_ID = ?) WHERE RN = 1" +
		") V ON K.STREAM_ID = ? AND K.ROUTE_ID = ? AND K.PK = V.PK" +
		" WHEN MATCHED AND (V.SOURCE_LSN > K.SOURCE_LSN OR (V.SOURCE_LSN = K.SOURCE_LSN AND V.SOURCE_SEQUENCE >= K.SOURCE_SEQUENCE))" +
		" THEN UPDATE SET SOURCE_LSN = V.SOURCE_LSN, SOURCE_SEQUENCE = V.SOURCE_SEQUENCE, DELETED = V.DELETED, UPDATED_AT = CURRENT_TIMESTAMP()" +
		" WHEN NOT MATCHED THEN INSERT (STREAM_ID, ROUTE_ID, PK, SOURCE_LSN, SOURCE_SEQUENCE, DELETED) VALUES (?, ?, V.PK, V.SOURCE_LSN, V.SOURCE_SEQUENCE, V.DELETED)"
}

// mergeSnapshotSQL applies one LOW/HIGH-bracketed source scan. A key clock
// newer than LOW proves CDC changed that key while (or after) it was scanned;
// excluding the candidate makes the already-applied CDC value or tombstone
// win. Clocks at or before LOW are safe because the scan happened after LOW.
func mergeSnapshotSQL(cfg Config, source *schema.Schema, table string) (string, error) {
	if !identifierPattern.MatchString(table) {
		return "", fmt.Errorf("invalid Snowflake target table %q", table)
	}
	pk := source.PKColumn()
	if pk == nil {
		return "", fmt.Errorf("source schema has no primary key")
	}
	columnNames := make([]string, 0, len(source.Columns))
	values := make([]string, 0, len(source.Columns))
	updates := make([]string, 0, len(source.Columns)-1)
	projection := make([]string, 0, len(source.Columns))
	for _, column := range source.Columns {
		name := quoteIdentifier(column.Name)
		expression, err := valueExpression(column, "S.PAYLOAD")
		if err != nil {
			return "", err
		}
		columnNames = append(columnNames, name)
		values = append(values, "V."+name)
		projection = append(projection, expression+" AS "+name)
		if !column.PrimaryKey {
			updates = append(updates, "D."+name+" = V."+name)
		}
	}
	using := "SELECT " + strings.Join(projection, ", ") +
		" FROM " + cfg.internal("SNAPSHOT_STAGE") + " S" +
		" LEFT JOIN " + cfg.internal("KEY_CLOCKS") + " K ON K.STREAM_ID = ? AND K.ROUTE_ID = ? AND K.PK = S.PK" +
		" WHERE S.STREAM_ID = ? AND S.JOB_ID = ? AND S.ATTEMPT = ? AND S.CHUNK_MIN = ?" +
		" AND (K.SOURCE_LSN IS NULL OR K.SOURCE_LSN <= ?)"
	updateClause := ""
	if len(updates) > 0 {
		updateClause = " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ", ")
	}
	return "MERGE INTO " + cfg.target(table) + " D USING (" + using + ") V ON D." + quoteIdentifier(pk.Name) + " = V." + quoteIdentifier(pk.Name) +
		updateClause + " WHEN NOT MATCHED THEN INSERT (" + strings.Join(columnNames, ", ") + ") VALUES (" + strings.Join(values, ", ") + ")", nil
}
