package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// RetainedArtifact is an identity-only projection, without run input/output.
// Empty checkpoint fields mean admission committed before the first checkpoint.
type RetainedArtifact struct {
	RunID              string
	ArtifactDigest     string
	ManifestJSON       json.RawMessage
	CheckpointArtifact string
	CheckpointDigest   string
	CheckpointPresent  bool
}

// RetainedArtifacts reads one consistent cut of all retained runs and
// checkpoints, including terminal runs and checkpoints left after compaction.
// Missing artifact records remain visible rather than disappearing in a join.
// visit must not access the database; rows are streamed to bound memory use.
func (j *Journal) RetainedArtifacts(ctx context.Context, visit func(RetainedArtifact) error) error {
	if visit == nil {
		return errors.New("journal: inventory visitor required")
	}
	return j.withRead(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT r.run_id, r.artifact_digest, a.manifest_json,
			COALESCE(c.artifact_digest, ''), COALESCE(c.checkpoint_digest, ''), c.run_id IS NOT NULL
			FROM journal_runs r LEFT JOIN journal_artifacts a ON a.digest = r.artifact_digest
			LEFT JOIN journal_checkpoints c ON c.run_id = r.run_id
			UNION ALL
			SELECT c.run_id, c.artifact_digest, a.manifest_json, c.artifact_digest, c.checkpoint_digest, 1
			FROM journal_checkpoints c LEFT JOIN journal_artifacts a ON a.digest = c.artifact_digest
			WHERE NOT EXISTS (SELECT 1 FROM journal_runs r WHERE r.run_id = c.run_id)`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item RetainedArtifact
			var manifest []byte
			if err := rows.Scan(&item.RunID, &item.ArtifactDigest, &manifest, &item.CheckpointArtifact, &item.CheckpointDigest, &item.CheckpointPresent); err != nil {
				return err
			}
			item.ManifestJSON = append([]byte(nil), manifest...)
			if err := visit(item); err != nil {
				return err
			}
		}
		return rows.Err()
	})
}
