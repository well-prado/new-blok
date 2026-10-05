package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
)

type inspectionLogHandler struct {
	emit  func(inspection.Event)
	step  string
	attrs []slog.Attr
}

func (h *inspectionLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *inspectionLogHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := append([]slog.Attr(nil), h.attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		if len(attrs) < 32 {
			attrs = append(attrs, attr)
		}
		return len(attrs) < 32
	})
	values := make(map[string]any, len(attrs))
	nodes := 0
	for _, attr := range attrs {
		attr.Key = truncateLogN(attr.Key, 128)
		if attr.Key == "" {
			if attr.Value.Kind() == slog.KindGroup {
				for i, child := range attr.Value.Group() {
					if i >= 32 {
						break
					}
					values[truncateLogN(child.Key, 128)] = safeLogValue(child.Value, 0, &nodes)
				}
			}
			continue
		}
		values[attr.Key] = safeLogValue(attr.Value, 0, &nodes)
	}
	raw, _ := json.Marshal(values)
	h.emit(inspection.Event{Kind: inspection.StepLog, StepID: h.step, At: record.Time, LogLevel: record.Level.String(), LogMessage: record.Message, LogAttrs: raw})
	return nil
}

func safeLogValue(value slog.Value, depth int, nodes *int) any {
	if depth >= 4 || *nodes >= 64 {
		return "[omitted]"
	}
	*nodes++
	value = value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		return truncateLogN(value.String(), 256)
	case slog.KindBool:
		return value.Bool()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindUint64:
		return value.Uint64()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindTime:
		return value.Time().UTC().Format(time.RFC3339Nano)
	case slog.KindGroup:
		group := map[string]any{}
		for i, child := range value.Group() {
			if i >= 32 {
				break
			}
			group[truncateLogN(child.Key, 128)] = safeLogValue(child.Value, depth+1, nodes)
		}
		return group
	case slog.KindAny:
		switch v := value.Any().(type) {
		case nil:
			return nil
		case string:
			return truncateLogN(v, 256)
		case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			return v
		case time.Time:
			return v.UTC().Format(time.RFC3339Nano)
		case time.Duration:
			return v.String()
		default:
			return "[omitted]"
		}
	default:
		return "[omitted]"
	}
}

func truncateLog(value string) string { return truncateLogN(value, 1024) }
func truncateLogN(value string, max int) string {
	if len(value) > max {
		return value[:max]
	}
	return value
}
func (h *inspectionLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copy := *h
	copy.attrs = append([]slog.Attr(nil), h.attrs...)
	remaining := 32 - len(copy.attrs)
	if remaining > 0 && len(attrs) > 0 {
		if len(attrs) < remaining {
			remaining = len(attrs)
		}
		copy.attrs = append(copy.attrs, attrs[:remaining]...)
	}
	return &copy
}
func (h *inspectionLogHandler) WithGroup(name string) slog.Handler {
	copy := *h
	copy.attrs = append([]slog.Attr(nil), h.attrs...)
	if len(copy.attrs) < 32 {
		copy.attrs = append(copy.attrs, slog.Group(truncateLogN(name, 128)))
	}
	return &copy
}
