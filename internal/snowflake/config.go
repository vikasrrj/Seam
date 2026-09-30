// Package snowflake implements SEAM's warehouse data plane.
//
// The control plane may lag or be unavailable without making committed
// warehouse data ambiguous: every source transaction is applied together
// with its Kafka frontier and source-transaction identity in one Snowflake
// DML transaction.
package snowflake

import (
	"fmt"
	"regexp"
	"strings"

	"example.com/seam/internal/schema"
)

var identifierPattern = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_$]*$")

// Config identifies the Snowflake objects owned by one SEAM pipeline.
type Config struct {
	Database       string
	Schema         string
	InternalSchema string
	LiveTable      string
	StreamID       string
	TopicID        string
	Partition      int32
}

func (c Config) validate() error {
	for label, value := range map[string]string{
		"database": c.Database, "schema": c.Schema,
		"internal schema": c.InternalSchema, "live table": c.LiveTable,
	} {
		if !identifierPattern.MatchString(value) {
			return fmt.Errorf("invalid Snowflake %s identifier %q", label, value)
		}
	}
	if strings.TrimSpace(c.StreamID) == "" {
		return fmt.Errorf("Snowflake stream ID is required")
	}
	if strings.TrimSpace(c.TopicID) == "" {
		return fmt.Errorf("Kafka topic ID is required")
	}
	if c.Partition != 0 {
		return fmt.Errorf("SEAM currently requires Kafka partition 0, got %d", c.Partition)
	}
	return nil
}

func quoteIdentifier(value string) string {
	return "\"" + strings.ToUpper(value) + "\""
}

func (c Config) object(schemaName, objectName string) string {
	return quoteIdentifier(c.Database) + "." + quoteIdentifier(schemaName) + "." + quoteIdentifier(objectName)
}

func (c Config) internal(objectName string) string { return c.object(c.InternalSchema, objectName) }

func (c Config) target(table string) string { return c.object(c.Schema, table) }

// livePhysicalTable is the initial writable generation behind the stable
// public LiveTable view. Promotions replace the view target idempotently;
// CDC never writes through the view.
func (c Config) livePhysicalTable() string { return c.LiveTable + "__SEAM_BASE" }

// ValidateSourceSchema returns an error before any row is consumed when a
// PostgreSQL type has no deliberately lossless Snowflake mapping. Expanding
// this closed set requires a round-trip test, not merely adding SQL syntax.
func ValidateSourceSchema(source *schema.Schema) error {
	if source == nil {
		return fmt.Errorf("nil source schema")
	}
	if source.PKColumn() == nil {
		return fmt.Errorf("source schema has no primary key")
	}
	for _, column := range source.Columns {
		if _, err := snowflakeType(column); err != nil {
			return fmt.Errorf("column %q: %w", column.Name, err)
		}
	}
	return nil
}

func snowflakeType(column schema.Column) (string, error) {
	switch column.TypeOID {
	case 20, 21, 23:
		return "NUMBER(38,0)", nil
	case 16:
		return "BOOLEAN", nil
	case 18, 25, 1042, 1043, 2950:
		return "VARCHAR", nil
	case 1082:
		return "DATE", nil
	case 1114:
		return "TIMESTAMP_NTZ(6)", nil
	case 1184:
		return "TIMESTAMP_TZ(6)", nil
	case 3802:
		return "VARIANT", nil
	default:
		return "", fmt.Errorf("PostgreSQL type %s (OID %d) has no proven lossless Snowflake mapping", column.TypeName, column.TypeOID)
	}
}

func valueExpression(column schema.Column, payload string) (string, error) {
	// VARIANT object keys are case-sensitive when quoted. PostgreSQL column
	// names are encoded into the staged JSON with their original spelling, so
	// upper-casing them like Snowflake SQL identifiers would read a missing key
	// and silently project NULL for every lower-case source column.
	path := payload + ":\"" + column.Name + "\""
	switch column.TypeOID {
	case 20, 21, 23:
		return "TO_NUMBER(" + path + "::VARCHAR)", nil
	case 16:
		return "TO_BOOLEAN(" + path + "::VARCHAR)", nil
	case 18, 25, 1042, 1043, 2950:
		return path + "::VARCHAR", nil
	case 1082:
		return "TO_DATE(" + path + "::VARCHAR)", nil
	case 1114:
		return "TO_TIMESTAMP_NTZ(" + path + "::VARCHAR)", nil
	case 1184:
		return "TO_TIMESTAMP_TZ(" + path + "::VARCHAR)", nil
	case 3802:
		return "PARSE_JSON(" + path + "::VARCHAR)", nil
	default:
		return "", fmt.Errorf("PostgreSQL type %s (OID %d) has no Snowflake expression", column.TypeName, column.TypeOID)
	}
}
