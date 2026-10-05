package promrule

import (
	"fmt"
	"strconv"
	"strings"
)

type yamlLine struct {
	number int
	indent int
	text   string // without indentation; comments kept for block scalars
}

// parseYAML parses the block-style YAML subset rule files use: nested
// mappings and sequences by indentation (spaces only), plain, single- and
// double-quoted scalars, and | and > block scalars with an optional - or +
// chomping indicator. Flow collections other than {} and [], anchors,
// aliases, tags, multiple documents and tabs are refused.
func parseYAML(input string) (any, error) {
	var lines []yamlLine
	for i, raw := range strings.Split(input, "\n") {
		if strings.Contains(raw, "\t") {
			return nil, fmt.Errorf("line %d: tabs are not supported", i+1)
		}
		trimmed := strings.TrimLeft(raw, " ")
		if trimmed == "---" && len(lines) == 0 {
			continue
		}
		lines = append(lines, yamlLine{number: i + 1, indent: len(raw) - len(trimmed), text: strings.TrimRight(trimmed, " \r")})
	}
	p := &yamlParser{lines: lines}
	p.skipBlank()
	if p.pos >= len(p.lines) {
		return nil, fmt.Errorf("empty document")
	}
	value, err := p.node(p.lines[p.pos].indent)
	if err != nil {
		return nil, err
	}
	p.skipBlank()
	if p.pos < len(p.lines) {
		return nil, fmt.Errorf("line %d: unexpected content", p.lines[p.pos].number)
	}
	return value, nil
}

type yamlParser struct {
	lines []yamlLine
	pos   int
}

func blank(l yamlLine) bool { return l.text == "" || strings.HasPrefix(l.text, "#") }

func (p *yamlParser) skipBlank() {
	for p.pos < len(p.lines) && blank(p.lines[p.pos]) {
		p.pos++
	}
}

func (p *yamlParser) node(indent int) (any, error) {
	p.skipBlank()
	if p.pos >= len(p.lines) {
		return nil, nil
	}
	l := p.lines[p.pos]
	if l.indent != indent {
		return nil, fmt.Errorf("line %d: unexpected indentation", l.number)
	}
	if l.text == "-" || strings.HasPrefix(l.text, "- ") {
		return p.sequence(indent)
	}
	return p.mapping(indent, nil)
}

func (p *yamlParser) sequence(indent int) (any, error) {
	var out []any
	for {
		p.skipBlank()
		if p.pos >= len(p.lines) || p.lines[p.pos].indent != indent || !(p.lines[p.pos].text == "-" || strings.HasPrefix(p.lines[p.pos].text, "- ")) {
			return out, nil
		}
		l := p.lines[p.pos]
		rest := strings.TrimPrefix(strings.TrimPrefix(l.text, "-"), " ")
		if strings.TrimSpace(rest) == "" {
			p.pos++
			item, err := p.node(p.childIndent(indent))
			if err != nil {
				return nil, err
			}
			out = append(out, item)
			continue
		}
		itemIndent := indent + (len(l.text) - len(strings.TrimLeft(strings.TrimPrefix(l.text, "-"), " "))) + 0
		if key, _, isMap := splitKey(rest); isMap && key != "" {
			// The item is a mapping whose first key shares the dash's line.
			p.lines[p.pos] = yamlLine{number: l.number, indent: itemIndent, text: rest}
			item, err := p.mapping(itemIndent, nil)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
			continue
		}
		scalar, err := scalarValue(rest, l.number)
		if err != nil {
			return nil, err
		}
		p.pos++
		out = append(out, scalar)
	}
}

func (p *yamlParser) childIndent(parent int) int {
	p.skipBlank()
	if p.pos < len(p.lines) && p.lines[p.pos].indent > parent {
		return p.lines[p.pos].indent
	}
	return parent + 1
}

// splitKey splits "key: value" outside quotes.
func splitKey(text string) (key, value string, ok bool) {
	if strings.HasPrefix(text, "\"") || strings.HasPrefix(text, "'") {
		return "", "", false
	}
	for i := 0; i < len(text); i++ {
		if text[i] == ':' && (i+1 == len(text) || text[i+1] == ' ') {
			return strings.TrimSpace(text[:i]), strings.TrimSpace(text[i+1:]), true
		}
		if text[i] == ' ' && i+1 < len(text) && text[i+1] == '#' {
			return "", "", false
		}
	}
	return "", "", false
}

func (p *yamlParser) mapping(indent int, _ any) (any, error) {
	out := map[string]any{}
	for {
		p.skipBlank()
		if p.pos >= len(p.lines) || p.lines[p.pos].indent < indent {
			return out, nil
		}
		l := p.lines[p.pos]
		if l.indent != indent {
			return nil, fmt.Errorf("line %d: unexpected indentation", l.number)
		}
		key, rest, ok := splitKey(l.text)
		if !ok || key == "" {
			return nil, fmt.Errorf("line %d: expected key: value", l.number)
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("line %d: duplicate key %q", l.number, key)
		}
		p.pos++
		switch {
		case rest == "" || strings.HasPrefix(rest, "#"):
			p.skipBlank()
			if p.pos < len(p.lines) && (p.lines[p.pos].indent > indent || p.lines[p.pos].indent == indent && strings.HasPrefix(p.lines[p.pos].text, "- ")) {
				child, err := p.node(p.lines[p.pos].indent)
				if err != nil {
					return nil, err
				}
				out[key] = child
			} else {
				out[key] = nil
			}
		case rest[0] == '|' || rest[0] == '>':
			value, err := p.blockScalar(indent, rest, l.number)
			if err != nil {
				return nil, err
			}
			out[key] = value
		default:
			value, err := scalarValue(rest, l.number)
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
	}
}

func (p *yamlParser) blockScalar(parent int, header string, number int) (string, error) {
	style, chomp := header[0], byte(0)
	if len(header) > 1 {
		chomp = header[1]
		if (chomp != '-' && chomp != '+') || strings.TrimSpace(header[2:]) != "" && !strings.HasPrefix(strings.TrimSpace(header[2:]), "#") {
			return "", fmt.Errorf("line %d: unsupported block scalar header %q", number, header)
		}
	}
	var body []string
	indent := -1
	for p.pos < len(p.lines) {
		l := p.lines[p.pos]
		if l.text == "" {
			body = append(body, "")
			p.pos++
			continue
		}
		if l.indent <= parent {
			break
		}
		if indent < 0 {
			indent = l.indent
		}
		if l.indent < indent {
			return "", fmt.Errorf("line %d: block scalar indentation decreased", l.number)
		}
		body = append(body, strings.Repeat(" ", l.indent-indent)+l.text)
		p.pos++
	}
	trailing := 0
	for len(body) > 0 && body[len(body)-1] == "" {
		body = body[:len(body)-1]
		trailing++
	}
	var text string
	if style == '|' {
		text = strings.Join(body, "\n")
	} else {
		var b strings.Builder
		for i, line := range body {
			switch {
			case i == 0:
			case line == "" || body[i-1] == "":
				b.WriteByte('\n')
			default:
				b.WriteByte(' ')
			}
			b.WriteString(line)
		}
		text = b.String()
	}
	switch chomp {
	case '-':
	case '+':
		text += strings.Repeat("\n", trailing+1)
	default:
		text += "\n"
	}
	return text, nil
}

func scalarValue(text string, number int) (any, error) {
	switch {
	case text == "{}":
		return map[string]any{}, nil
	case text == "[]":
		return []any{}, nil
	case strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[") || strings.HasPrefix(text, "&") || strings.HasPrefix(text, "*") || strings.HasPrefix(text, "!"):
		return nil, fmt.Errorf("line %d: flow collections, anchors, aliases and tags are not supported", number)
	case strings.HasPrefix(text, "\""):
		end := strings.LastIndexByte(text, '"')
		if end == 0 {
			return nil, fmt.Errorf("line %d: unterminated string", number)
		}
		if rest := strings.TrimSpace(text[end+1:]); rest != "" && !strings.HasPrefix(rest, "#") {
			return nil, fmt.Errorf("line %d: content after string", number)
		}
		value, err := strconv.Unquote(text[:end+1])
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", number, err)
		}
		return value, nil
	case strings.HasPrefix(text, "'"):
		end := strings.LastIndexByte(text, '\'')
		if end == 0 {
			return nil, fmt.Errorf("line %d: unterminated string", number)
		}
		if rest := strings.TrimSpace(text[end+1:]); rest != "" && !strings.HasPrefix(rest, "#") {
			return nil, fmt.Errorf("line %d: content after string", number)
		}
		return strings.ReplaceAll(text[1:end], "''", "'"), nil
	}
	if i := strings.Index(text, " #"); i >= 0 {
		text = strings.TrimSpace(text[:i])
	}
	return text, nil
}
