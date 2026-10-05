package journal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
)

// RunOwner names the durable owner of runID for reader. The journal
// authorizes inspection by exact principal, so the owner it can name is the
// reader itself, and only for a run the reader owns.
func (j *Journal) RunOwner(ctx context.Context, reader, runID string) (string, error) {
	if reader == "" || runID == "" {
		return "", ErrNotFound
	}
	var owner string
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT principal FROM journal_runs WHERE run_id=? AND principal=?`, runID, reader).Scan(&owner); err != nil {
			return ErrNotFound
		}
		return nil
	})
	return owner, err
}

// ReadInspection authorizes before materializing payloads and performs the
// bounded step selection in SQL. At most 200 steps and 100 attempts per step
// are read for one request.
func (j *Journal) ReadInspection(ctx context.Context, principal, runID, stepID string, offset, limit int, fields map[inspection.Field]bool, maxPayload int) (inspection.Run, []inspection.Step, int, error) {
	if principal == "" || runID == "" || offset < 0 || limit < 1 || limit > 200 || maxPayload < inspection.MinPayloadBytes {
		return inspection.Run{}, nil, 0, ErrNotFound
	}
	var run inspection.Run
	var steps []inspection.Step
	var total int
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var created int64
		var completed sql.NullInt64
		var input, output []byte
		var runErrorCode, runErrorClass string
		var workflow, state string
		inputProjection, outputProjection := `NULL`, `NULL`
		if fields[inspection.FieldInput] {
			inputProjection = `CASE WHEN length(input_json)<=? THEN input_json ELSE CAST('{"$truncated":true}' AS BLOB) END`
		}
		if fields[inspection.FieldOutput] {
			outputProjection = `CASE WHEN output_json IS NULL THEN NULL WHEN length(output_json)<=? THEN output_json ELSE CAST('{"$truncated":true}' AS BLOB) END`
		}
		query := fmt.Sprintf(`SELECT workflow,state,created_at,completed_at,%s,%s,error_code,error_class FROM journal_runs WHERE run_id=? AND principal=?`, inputProjection, outputProjection)
		args := make([]any, 0, 4)
		if fields[inspection.FieldInput] {
			args = append(args, maxPayload)
		}
		if fields[inspection.FieldOutput] {
			args = append(args, maxPayload)
		}
		args = append(args, runID, principal)
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&workflow, &state, &created, &completed, &input, &output, &runErrorCode, &runErrorClass); err != nil {
			return ErrNotFound
		}
		run = inspection.Run{ID: runID, Workflow: workflow, Status: inspection.StatusRunning, StartedAt: time.Unix(0, created).UTC(), Input: append([]byte(nil), input...), Output: append([]byte(nil), output...), ErrorCode: runErrorCode, ErrorClass: runErrorClass}
		if completed.Valid {
			run.FinishedAt = time.Unix(0, completed.Int64).UTC()
		}
		switch state {
		case runCompleted:
			run.Status = inspection.StatusCompleted
		case runCanceled:
			run.Status = inspection.StatusCanceled
		case runFailed:
			run.Status = inspection.StatusFailed
		case runUncertain:
			run.Status = inspection.StatusUncertain
		}
		var waiting, uncertain int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_waits WHERE run_id=? AND state=?`, runID, waitWaiting).Scan(&waiting); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_operations WHERE run_id=? AND state=?`, runID, operationUncertain).Scan(&uncertain); err != nil {
			return err
		}
		if uncertain > 0 {
			run.Status = inspection.StatusUncertain
		} else if run.Status == inspection.StatusRunning && waiting > 0 {
			run.Status = inspection.StatusSuspended
		}
		_ = tx.QueryRowContext(ctx, `SELECT c.run_id,c.path FROM journal_children c JOIN journal_runs p ON p.run_id=c.run_id WHERE c.child_run_id=? AND p.principal=? LIMIT 1`, runID, principal).Scan(&run.ParentRun, &run.ParentStep)
		paths := `WITH paths(path,iter) AS (SELECT path,'' FROM journal_scopes WHERE run_id=? UNION SELECT invocation_path,iteration_path FROM journal_operations WHERE run_id=? UNION SELECT 'wait:'||name,'' FROM journal_waits WHERE run_id=?) `
		if stepID == "" {
			if err := tx.QueryRowContext(ctx, paths+`SELECT COUNT(*) FROM paths`, runID, runID, runID).Scan(&total); err != nil {
				return err
			}
		} else if err := tx.QueryRowContext(ctx, paths+`SELECT COUNT(*) FROM paths WHERE CASE WHEN iter='' THEN path ELSE path||'['||iter||']' END=?`, runID, runID, runID, stepID).Scan(&total); err != nil {
			return err
		}
		pathsQuery, pathsArgs := paths+`SELECT path,iter FROM paths`, []any{runID, runID, runID}
		if stepID != "" {
			pathsQuery += ` WHERE CASE WHEN iter='' THEN path ELSE path||'['||iter||']' END=?`
			pathsArgs = append(pathsArgs, stepID)
		}
		pathsQuery += ` ORDER BY path,iter LIMIT ? OFFSET ?`
		pathsArgs = append(pathsArgs, limit, offset)
		rows, err := tx.QueryContext(ctx, pathsQuery, pathsArgs...)
		if err != nil {
			return err
		}
		type path struct{ name, iteration string }
		selected := make([]path, 0, limit)
		for rows.Next() {
			var item path
			if err := rows.Scan(&item.name, &item.iteration); err != nil {
				rows.Close()
				return err
			}
			selected = append(selected, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		steps = make([]inspection.Step, 0, len(selected))
		for _, item := range selected {
			id := item.name
			if item.iteration != "" {
				id += "[" + item.iteration + "]"
			}
			step := inspection.Step{ID: id, Status: inspection.StatusRunning}
			if len(item.name) > 5 && item.name[:5] == "wait:" {
				var waitState string
				var payload []byte
				var updated int64
				payloadProjection := `NULL`
				if fields[inspection.FieldOutput] {
					payloadProjection = `CASE WHEN payload_json IS NULL THEN NULL WHEN length(payload_json)<=? THEN payload_json ELSE CAST('{"$truncated":true}' AS BLOB) END`
				}
				query := fmt.Sprintf(`SELECT state,%s,created_at FROM journal_waits WHERE run_id=? AND name=?`, payloadProjection)
				args := []any{runID, item.name[5:]}
				if fields[inspection.FieldOutput] {
					args = []any{maxPayload, runID, item.name[5:]}
				}
				if err := tx.QueryRowContext(ctx, query, args...).Scan(&waitState, &payload, &updated); err == nil {
					step.StartedAt = time.Unix(0, updated).UTC()
					step.Output = append([]byte(nil), payload...)
					if waitState == waitResumed {
						step.Status = inspection.StatusCompleted
						step.FinishedAt = step.StartedAt
					} else if waitState == waitWaiting {
						step.Status = inspection.StatusSuspended
					}
				}
				steps = append(steps, step)
				continue
			}
			var scopeState string
			var scopeInput, scopeOutput []byte
			var updated int64
			scopeInputProjection := `NULL`
			if fields[inspection.FieldInput] {
				scopeInputProjection = `CASE WHEN input_json IS NULL THEN NULL WHEN length(input_json)<=? THEN input_json ELSE CAST('{"$truncated":true}' AS BLOB) END`
			}
			scopeProjection := `NULL`
			if fields[inspection.FieldOutput] {
				scopeProjection = `CASE WHEN output_json IS NULL THEN NULL WHEN length(output_json)<=? THEN output_json ELSE CAST('{"$truncated":true}' AS BLOB) END`
			}
			scopeQuery := fmt.Sprintf(`SELECT state,%s,%s,updated_at FROM journal_scopes WHERE run_id=? AND path=?`, scopeInputProjection, scopeProjection)
			scopeArgs := []any{runID, item.name}
			if fields[inspection.FieldInput] {
				scopeArgs = []any{maxPayload, runID, item.name}
			}
			if fields[inspection.FieldOutput] {
				scopeArgs = append([]any{maxPayload}, scopeArgs...)
			}
			err := tx.QueryRowContext(ctx, scopeQuery, scopeArgs...).Scan(&scopeState, &scopeInput, &scopeOutput, &updated)
			if err == nil {
				step.StartedAt = time.Unix(0, updated).UTC()
				step.Input = append([]byte(nil), scopeInput...)
				step.Output = append([]byte(nil), scopeOutput...)
				switch scopeState {
				case checkpointCompleted:
					step.Status = inspection.StatusCompleted
					step.FinishedAt = step.StartedAt
				case checkpointCanceled:
					step.Status = inspection.StatusCanceled
				}
			}
			attemptInput, attemptOutput := `NULL`, `NULL`
			attemptArgs := make([]any, 0, 6)
			if fields[inspection.FieldInput] {
				attemptInput = `CASE WHEN a.input_json IS NULL THEN NULL WHEN length(a.input_json)<=? THEN a.input_json ELSE CAST('{"$truncated":true}' AS BLOB) END`
				attemptArgs = append(attemptArgs, maxPayload)
			}
			if fields[inspection.FieldOutput] {
				attemptOutput = `CASE WHEN a.result_json IS NULL THEN NULL WHEN length(a.result_json)<=? THEN a.result_json ELSE CAST('{"$truncated":true}' AS BLOB) END`
				attemptArgs = append(attemptArgs, maxPayload)
			}
			attemptArgs = append(attemptArgs, runID, item.name, item.iteration, inspection.MaxProjectedAttempts+1)
			attemptQuery := fmt.Sprintf(`SELECT a.attempt_id,a.attempt_number,a.state,%s,%s,a.started_at,a.finished_at FROM journal_attempts a JOIN journal_operations o ON o.operation_key=a.operation_key WHERE o.run_id=? AND o.invocation_path=? AND o.iteration_path=? ORDER BY a.attempt_number DESC LIMIT ?`, attemptInput, attemptOutput)
			attemptRows, err := tx.QueryContext(ctx, attemptQuery, attemptArgs...)
			if err != nil {
				return err
			}
			for attemptRows.Next() {
				var a inspection.Attempt
				var input, result []byte
				var started int64
				var finished sql.NullInt64
				var attemptState string
				if err := attemptRows.Scan(&a.ID, &a.Number, &attemptState, &input, &result, &started, &finished); err != nil {
					attemptRows.Close()
					return err
				}
				a.StartedAt = time.Unix(0, started).UTC()
				if finished.Valid {
					a.FinishedAt = time.Unix(0, finished.Int64).UTC()
				}
				a.Input = append([]byte(nil), input...)
				a.Output = append([]byte(nil), result...)
				switch attemptState {
				case attemptCommitted:
					a.Status = inspection.StatusCompleted
				case attemptUncertain:
					a.Status = inspection.StatusUncertain
				case attemptFailed:
					a.Status = inspection.StatusFailed
				default:
					a.Status = inspection.StatusRunning
				}
				if len(step.Attempts) < inspection.MaxProjectedAttempts {
					step.Attempts = append(step.Attempts, a)
				} else {
					step.AttemptsTruncated = true
				}
			}
			if err := attemptRows.Err(); err != nil {
				attemptRows.Close()
				return err
			}
			attemptRows.Close()
			for left, right := 0, len(step.Attempts)-1; left < right; left, right = left+1, right-1 {
				step.Attempts[left], step.Attempts[right] = step.Attempts[right], step.Attempts[left]
			}
			if len(step.Attempts) > 0 {
				last := step.Attempts[len(step.Attempts)-1]
				step.Attempt = last.Number
				step.Status = last.Status
				step.StartedAt = last.StartedAt
				step.FinishedAt = last.FinishedAt
				if len(step.Input) == 0 {
					step.Input = append([]byte(nil), last.Input...)
				}
				step.Output = append([]byte(nil), last.Output...)
			}
			steps = append(steps, step)
		}
		return nil
	})
	if err != nil {
		return inspection.Run{}, nil, 0, fmt.Errorf("journal: inspect: %w", err)
	}
	return run, steps, total, nil
}

var _ inspection.Source = (*Journal)(nil)
