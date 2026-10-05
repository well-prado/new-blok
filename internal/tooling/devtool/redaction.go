package devtool

import (
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/observe/redact"
)

// redactLines redacts output as a block, not only line by line, because a
// credential can span lines:
//
//   - a PEM block, from its BEGIN line to its END line (or to the end of
//     the output when the END line was not kept), becomes one marker line;
//   - every other line goes through redact.Message;
//   - if the remaining lines, read together, still look sensitive to
//     observe/redact (JSON or an encoding split across lines), the whole
//     block becomes one marker line.
func redactLines(lines []string) []string {
	if len(lines) == 0 {
		return nil
	}
	result := make([]string, 0, len(lines))
	inPEM := false
	for _, line := range lines {
		begins, ends := strings.Contains(line, "-----BEGIN "), strings.Contains(line, "-----END ")
		switch {
		case begins:
			if !inPEM {
				result = append(result, redact.MessageMarker)
			}
			inPEM = !ends
		case inPEM:
			inPEM = !ends
		default:
			result = append(result, redact.Message(line))
		}
	}
	if redact.Sensitive(strings.Join(result, "\n")) {
		return []string{redact.MessageMarker}
	}
	return result
}

// redactDiagnostic passes every free-text field of a diagnostic through the
// redaction boundary. Every report's diagnostics go through it, whatever
// produced them: compiler text, test output and names, go's standard error.
func redactDiagnostic(item diagnostic.Diagnostic) diagnostic.Diagnostic {
	item.Source = redact.String(item.Source)
	item.Step = redact.String(item.Step)
	item.Field = redact.String(item.Field)
	item.Expected = redact.Message(item.Expected)
	item.Actual = redact.Message(item.Actual)
	item.Message = redact.Message(item.Message)
	return item
}
