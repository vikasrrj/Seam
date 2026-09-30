package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
)

// MarkerPosition returns the durable Kafka and source positions of one marker.
func (s *Store) MarkerPosition(ctx context.Context, markerID string) (int64, uint64, error) {
	var offset int64
	var lsnText string
	if err := s.db.QueryRowContext(ctx, "SELECT FINAL_OFFSET, SOURCE_LSN FROM "+s.cfg.internal("MARKERS")+" WHERE STREAM_ID = ? AND MARKER_ID = ?", s.cfg.StreamID, markerID).Scan(&offset, &lsnText); err != nil {
		return 0, 0, fmt.Errorf("read Snowflake marker %q position: %w", markerID, err)
	}
	lsn, err := strconv.ParseUint(lsnText, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("Snowflake marker %q has invalid source LSN %q: %w", markerID, lsnText, err)
	}
	return offset, lsn, nil
}

// PrepareValidationClone records validation intent before issuing Snowflake
// DDL, then creates an immutable zero-copy clone of the shadow at the marker
// boundary. Repeating the operation is idempotent for the same job/table.
func (s *Store) PrepareValidationClone(ctx context.Context, jobID, markerID, validationTable string) (*BackfillJob, error) {
	if !identifierPattern.MatchString(validationTable) {
		return nil, fmt.Errorf("invalid Snowflake validation table %q", validationTable)
	}
	offset, _, err := s.MarkerPosition(ctx, markerID)
	if err != nil {
		return nil, err
	}
	job, err := s.LoadBackfill(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job.State == BackfillCompleted {
		return job, nil
	}
	if job.State == BackfillVerifying {
		if !strings.EqualFold(job.ValidationTable, validationTable) {
			return nil, fmt.Errorf("Snowflake backfill %q already owns validation table %q", jobID, job.ValidationTable)
		}
		if job.ValidationMarkerID != markerID || job.ValidationOffset != offset {
			// A process crash destroys the exported PostgreSQL snapshot even when
			// the Snowflake clone survived. A new fenced marker safely supersedes
			// that unusable validation attempt while keeping the object name.
			result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET VALIDATION_MARKER_ID = ?, VALIDATION_OFFSET = ?, VALIDATION_READY = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE = ? AND VALIDATION_TABLE = ?", markerID, offset, s.cfg.StreamID, jobID, job.Spec.Attempt, string(BackfillVerifying), strings.ToUpper(validationTable))
			if err != nil {
				return nil, err
			}
			if affected, err := result.RowsAffected(); err != nil || affected != 1 {
				return nil, fmt.Errorf("restart Snowflake validation intent: job changed concurrently")
			}
			job.ValidationMarkerID = markerID
			job.ValidationOffset = offset
			job.ValidationReady = false
		}
	} else {
		if job.State != BackfillReadyToVerify {
			return nil, fmt.Errorf("Snowflake backfill %q is %s, expected %s", jobID, job.State, BackfillReadyToVerify)
		}
		result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET STATE = ?, VALIDATION_TABLE = ?, VALIDATION_MARKER_ID = ?, VALIDATION_OFFSET = ?, VALIDATION_READY = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE = ?", string(BackfillVerifying), strings.ToUpper(validationTable), markerID, offset, s.cfg.StreamID, jobID, job.Spec.Attempt, string(BackfillReadyToVerify))
		if err != nil {
			return nil, err
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return nil, fmt.Errorf("record Snowflake validation intent: job changed concurrently")
		}
		job.State = BackfillVerifying
		job.ValidationTable = strings.ToUpper(validationTable)
		job.ValidationMarkerID = markerID
		job.ValidationOffset = offset
	}
	if job.ValidationReady {
		return job, nil
	}
	// CREATE OR REPLACE is safe only because the durable intent above assigns
	// this exact object name to this exact job before DDL is attempted.
	cloneSQL := "CREATE OR REPLACE TRANSIENT TABLE " + s.cfg.target(validationTable) + " CLONE " + s.cfg.target(job.Spec.ShadowTable)
	if _, err := s.db.ExecContext(ctx, cloneSQL); err != nil {
		return nil, fmt.Errorf("clone Snowflake shadow for validation: %w", err)
	}
	result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET VALIDATION_READY = TRUE, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE = ? AND VALIDATION_TABLE = ? AND VALIDATION_MARKER_ID = ?", s.cfg.StreamID, jobID, job.Spec.Attempt, string(BackfillVerifying), strings.ToUpper(validationTable), markerID)
	if err != nil {
		return nil, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return nil, fmt.Errorf("mark Snowflake validation clone ready: job changed concurrently")
	}
	job.ValidationReady = true
	return job, nil
}

func (s *Store) MarkValidated(ctx context.Context, jobID string) error {
	result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET STATE = ?, VALIDATED_AT = CURRENT_TIMESTAMP(), UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND STATE = ? AND VALIDATION_READY = TRUE", string(BackfillReady), s.cfg.StreamID, jobID, string(BackfillVerifying))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("mark Snowflake backfill %q validated: state changed or clone is not ready", jobID)
	}
	return nil
}

func (s *Store) FailBackfill(ctx context.Context, jobID string, cause error) error {
	message := "unknown failure"
	if cause != nil {
		message = cause.Error()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var routeID, state string
	if err := tx.QueryRowContext(ctx, "SELECT SHADOW_ROUTE_ID, STATE FROM "+s.cfg.internal("BACKFILL_JOBS")+" WHERE STREAM_ID = ? AND JOB_ID = ?", s.cfg.StreamID, jobID).Scan(&routeID, &state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("Snowflake backfill %q does not exist", jobID)
		}
		return err
	}
	if BackfillState(state) == BackfillCompleted {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("ROUTES")+" SET ACTIVE = FALSE WHERE STREAM_ID = ? AND ROUTE_ID = ?", s.cfg.StreamID, routeID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET STATE = ?, ERROR_MESSAGE = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND STATE <> ?", string(BackfillFailed), message, s.cfg.StreamID, jobID, string(BackfillCompleted))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("fail Snowflake backfill %q: job changed concurrently", jobID)
	}
	return tx.Commit()
}

// PromoteView atomically changes the stable public view to the verified shadow
// and then converges CDC routing in one DML transaction. The view replacement
// is idempotent, so an ambiguous DDL result can be retried without undoing a
// successful promotion.
func (s *Store) PromoteView(ctx context.Context, jobID, markerID string) error {
	offset, _, err := s.MarkerPosition(ctx, markerID)
	if err != nil {
		return err
	}
	job, err := s.LoadBackfill(ctx, jobID)
	if err != nil {
		return err
	}
	if job.State == BackfillCompleted {
		if job.PromotionMarkerID != markerID || job.PromotionOffset != offset {
			return fmt.Errorf("Snowflake backfill %q was promoted at a different boundary", jobID)
		}
		return nil
	}
	if job.State == BackfillPromoting {
		if job.PromotionMarkerID != markerID || job.PromotionOffset != offset {
			return fmt.Errorf("Snowflake backfill %q already has different promotion intent", jobID)
		}
	} else {
		if job.State != BackfillReady {
			return fmt.Errorf("Snowflake backfill %q is %s, expected %s", jobID, job.State, BackfillReady)
		}
		result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET STATE = ?, PROMOTION_MARKER_ID = ?, PROMOTION_OFFSET = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE = ?", string(BackfillPromoting), markerID, offset, s.cfg.StreamID, jobID, job.Spec.Attempt, string(BackfillReady))
		if err != nil {
			return err
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("record Snowflake promotion intent: job changed concurrently")
		}
	}
	if _, err := s.db.ExecContext(ctx, createLiveViewSQL(s.cfg, s.schema, job.Spec.ShadowTable, true)); err != nil {
		return fmt.Errorf("atomically replace Snowflake live view: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("ROUTES")+" SET TARGET_TABLE = ?, ACTIVE = TRUE WHERE STREAM_ID = ? AND ROUTE_ID = 'live'", strings.ToUpper(job.Spec.ShadowTable), s.cfg.StreamID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("ROUTES")+" SET ACTIVE = FALSE WHERE STREAM_ID = ? AND ROUTE_ID = ?", s.cfg.StreamID, job.ShadowRouteID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET STATE = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE = ? AND PROMOTION_MARKER_ID = ? AND PROMOTION_OFFSET = ?", string(BackfillCompleted), s.cfg.StreamID, jobID, job.Spec.Attempt, string(BackfillPromoting), markerID, offset)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("complete Snowflake promotion: job changed concurrently")
	}
	return tx.Commit()
}

// ReadRows reads one key range from a physical Snowflake table into the same
// normalized row representation used by PostgreSQL snapshot validation.
func (s *Store) ReadRows(ctx context.Context, table string, minID, maxID int64) ([]model.Row, error) {
	if !identifierPattern.MatchString(table) {
		return nil, fmt.Errorf("invalid Snowflake validation table %q", table)
	}
	pk := s.schema.PKColumn()
	if pk == nil {
		return nil, fmt.Errorf("source schema has no primary key")
	}
	expressions := make([]string, len(s.schema.Columns))
	for index, column := range s.schema.Columns {
		expression, err := validationExpression(column)
		if err != nil {
			return nil, err
		}
		expressions[index] = expression
	}
	query := "SELECT " + strings.Join(expressions, ", ") + " FROM " + s.cfg.target(table) + " WHERE " + quoteIdentifier(pk.Name) + " >= ? AND " + quoteIdentifier(pk.Name) + " <= ? ORDER BY " + quoteIdentifier(pk.Name)
	rows, err := s.db.QueryContext(ctx, query, minID, maxID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []model.Row
	for rows.Next() {
		values := make([]*string, len(s.schema.Columns))
		destinations := make([]any, len(values))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		row := model.Row{Values: make([]model.Value, len(values))}
		for index, value := range values {
			if value == nil {
				row.Values[index] = model.NullValue()
				continue
			}
			normalized, err := normalizeValidationText(s.schema.Columns[index], *value)
			if err != nil {
				return nil, err
			}
			if s.schema.IsIntegerColumn(s.schema.Columns[index]) {
				integer, err := strconv.ParseInt(normalized, 10, 64)
				if err != nil {
					return nil, err
				}
				row.Values[index] = model.Int64Value(integer)
			} else {
				row.Values[index] = model.TextValue(normalized)
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) ReadKeys(ctx context.Context, table string, keys []int64) ([]model.Row, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if !identifierPattern.MatchString(table) {
		return nil, fmt.Errorf("invalid Snowflake validation table %q", table)
	}
	pk := s.schema.PKColumn()
	if pk == nil {
		return nil, fmt.Errorf("source schema has no primary key")
	}
	expressions := make([]string, len(s.schema.Columns))
	for index, column := range s.schema.Columns {
		expression, err := validationExpression(column)
		if err != nil {
			return nil, err
		}
		expressions[index] = expression
	}
	const keyBatch = 1000
	var result []model.Row
	for start := 0; start < len(keys); start += keyBatch {
		end := start + keyBatch
		if end > len(keys) {
			end = len(keys)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
		query := "SELECT " + strings.Join(expressions, ", ") + " FROM " + s.cfg.target(table) + " WHERE " + quoteIdentifier(pk.Name) + " IN (" + placeholders + ") ORDER BY " + quoteIdentifier(pk.Name)
		args := make([]any, end-start)
		for index, key := range keys[start:end] {
			args[index] = key
		}
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		decoded, err := s.scanValidationRows(rows)
		rows.Close()
		if err != nil {
			return nil, err
		}
		result = append(result, decoded...)
	}
	return result, nil
}

type sqlRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func (s *Store) scanValidationRows(rows sqlRows) ([]model.Row, error) {
	var result []model.Row
	for rows.Next() {
		values := make([]*string, len(s.schema.Columns))
		destinations := make([]any, len(values))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		row := model.Row{Values: make([]model.Value, len(values))}
		for index, value := range values {
			if value == nil {
				row.Values[index] = model.NullValue()
				continue
			}
			normalized, err := normalizeValidationText(s.schema.Columns[index], *value)
			if err != nil {
				return nil, err
			}
			if s.schema.IsIntegerColumn(s.schema.Columns[index]) {
				integer, err := strconv.ParseInt(normalized, 10, 64)
				if err != nil {
					return nil, err
				}
				row.Values[index] = model.Int64Value(integer)
			} else {
				row.Values[index] = model.TextValue(normalized)
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func NormalizeSourceRows(source *schema.Schema, rows []model.Row) ([]model.Row, error) {
	normalized := make([]model.Row, len(rows))
	for rowIndex, row := range rows {
		if len(row.Values) != len(source.Columns) {
			return nil, fmt.Errorf("source row has %d values, expected %d", len(row.Values), len(source.Columns))
		}
		normalized[rowIndex].Values = make([]model.Value, len(row.Values))
		for columnIndex, value := range row.Values {
			if value.Kind != model.ValueText {
				normalized[rowIndex].Values[columnIndex] = value
				continue
			}
			text, err := normalizeValidationText(source.Columns[columnIndex], value.Text)
			if err != nil {
				return nil, err
			}
			normalized[rowIndex].Values[columnIndex] = model.TextValue(text)
		}
	}
	return normalized, nil
}

func validationExpression(column schema.Column) (string, error) {
	name := quoteIdentifier(column.Name)
	switch column.TypeOID {
	case 20, 21, 23:
		return "TO_VARCHAR(" + name + ")", nil
	case 16:
		return "IFF(" + name + ", 't', 'f')", nil
	case 18, 25, 1042, 1043, 2950:
		return name, nil
	case 1082:
		return "TO_CHAR(" + name + ", 'YYYY-MM-DD')", nil
	case 1114:
		return "TO_CHAR(" + name + ", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')", nil
	case 1184:
		return "TO_CHAR(CONVERT_TIMEZONE('UTC', " + name + "), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9\"Z\"')", nil
	case 3802:
		return "TO_JSON(" + name + ")", nil
	default:
		return "", fmt.Errorf("column %q type %s cannot be exactly validated in Snowflake", column.Name, column.TypeName)
	}
}

func normalizeValidationText(column schema.Column, value string) (string, error) {
	switch column.TypeOID {
	case 1114:
		parsed, err := parseTimestampWithoutZone(value)
		if err != nil {
			return "", fmt.Errorf("normalize timestamp column %q value %q: %w", column.Name, value, err)
		}
		return parsed.Format("2006-01-02T15:04:05.999999999"), nil
	case 1184:
		parsed, err := parseTimestampWithZone(value)
		if err != nil {
			return "", fmt.Errorf("normalize timestamptz column %q value %q: %w", column.Name, value, err)
		}
		return parsed.UTC().Format(time.RFC3339Nano), nil
	case 3802:
		return canonicalJSON(value)
	default:
		return value, nil
	}
}

func parseTimestampWithoutZone(value string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp representation")
}

func parseTimestampWithZone(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999Z07"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamptz representation")
}

func canonicalJSON(value string) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return "", err
	}
	var builder strings.Builder
	if err := writeCanonicalJSON(&builder, decoded); err != nil {
		return "", err
	}
	return builder.String(), nil
}

func writeCanonicalJSON(builder *strings.Builder, value any) error {
	switch typed := value.(type) {
	case nil:
		builder.WriteString("null")
	case bool:
		if typed {
			builder.WriteString("true")
		} else {
			builder.WriteString("false")
		}
	case string:
		encoded, _ := json.Marshal(typed)
		builder.Write(encoded)
	case json.Number:
		rational := new(big.Rat)
		if _, ok := rational.SetString(string(typed)); !ok {
			return fmt.Errorf("invalid JSON number %q", typed)
		}
		builder.WriteString("n:")
		builder.WriteString(rational.RatString())
	case []any:
		builder.WriteByte('[')
		for index, child := range typed {
			if index > 0 {
				builder.WriteByte(',')
			}
			if err := writeCanonicalJSON(builder, child); err != nil {
				return err
			}
		}
		builder.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		builder.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				builder.WriteByte(',')
			}
			encoded, _ := json.Marshal(key)
			builder.Write(encoded)
			builder.WriteByte(':')
			if err := writeCanonicalJSON(builder, typed[key]); err != nil {
				return err
			}
		}
		builder.WriteByte('}')
	default:
		return fmt.Errorf("unsupported decoded JSON type %T", value)
	}
	return nil
}
