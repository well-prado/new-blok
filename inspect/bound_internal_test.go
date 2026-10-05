package inspect

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestBoundTruncatesBeforeRedactingOversizedPayloads: a payload over the
// redaction work cap is truncated without being decoded, even when its
// redacted form would have fit (a long secret shrinks to the marker). The
// reported size is the stored payload's.
func TestBoundTruncatesBeforeRedactingOversizedPayloads(t *testing.T) {
	raw, _ := json.Marshal(map[string]string{"note": "password=SYNTHETIC-" + strings.Repeat("x", 900<<10)})
	got := bound(raw, 64<<10)
	var projected map[string]any
	if err := json.Unmarshal(got, &projected); err != nil {
		t.Fatal(err)
	}
	if projected["$truncated"] != true || projected["originalBytes"] != float64(len(raw)) {
		t.Fatalf("oversized payload projected as %s, want a truncation of %d stored bytes", got, len(raw))
	}
	small, _ := json.Marshal(map[string]string{"note": "password=SYNTHETIC-" + strings.Repeat("x", 512)})
	if got := bound(small, 64<<10); string(got) != `{"note":"[redacted]"}` {
		t.Fatalf("payload within the cap projected as %s", got)
	}
}
