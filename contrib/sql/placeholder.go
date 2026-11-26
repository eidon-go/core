package sql

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// PlaceholderFormat rewrites the "?" placeholders of a query built by a
// query builder. Its method set is that of squirrel's PlaceholderFormat, so
// Dollar and Question can be passed to squirrel without this module
// depending on it:
//
//	psql := sq.StatementBuilder.PlaceholderFormat(sql.Dollar)
//
// The helpers (Get, Select, Exec) never rewrite queries: the builder does,
// when it renders them.
type PlaceholderFormat interface {
	ReplacePlaceholders(query string) (string, error)
}

//nolint:gochecknoglobals // stateless values passed to query builders, like squirrel's
var (
	// Dollar numbers placeholders: "?" becomes "$1", "$2", … (PostgreSQL).
	// "??" is an escaped "?" and becomes a literal "?", for operators such
	// as jsonb's "?", "?|" and "?&": write them "??", "??|", "??&". A "?"
	// inside a string literal ('…', E'…', $tag$…$tag$), a quoted identifier
	// ("…") or a comment (-- …, /* … */) is left as it is.
	Dollar PlaceholderFormat = dollarFormat{}

	// Question leaves "?" placeholders as they are (MySQL, SQLite).
	Question PlaceholderFormat = questionFormat{}
)

type dollarFormat struct{}

// ReplacePlaceholders implements PlaceholderFormat.
func (dollarFormat) ReplacePlaceholders(query string) (string, error) {
	if !strings.Contains(query, "?") {
		return query, nil
	}

	var out strings.Builder

	out.Grow(len(query) + 8) //nolint:mnd // room for a few multi-digit numbers

	n := 0

	for pos := 0; pos < len(query); {
		if end := skipQuoted(query, pos); end > pos {
			out.WriteString(query[pos:end])
			pos = end

			continue
		}

		switch {
		case strings.HasPrefix(query[pos:], "??"):
			out.WriteByte('?')

			pos += len("??")
		case query[pos] == '?':
			n++

			out.WriteByte('$')
			out.WriteString(strconv.Itoa(n))

			pos++
		default:
			out.WriteByte(query[pos])

			pos++
		}
	}

	return out.String(), nil
}

// skipQuoted returns the end of the literal, quoted identifier or comment
// that starts at pos, or pos when none starts there. An unterminated one runs
// to the end of the query.
func skipQuoted(query string, pos int) int {
	rest := query[pos:]

	switch {
	case rest[0] == '\'':
		return closeQuote(query, pos+1, '\'', isEscapeString(query, pos))
	case rest[0] == '"':
		return closeQuote(query, pos+1, '"', false)
	case strings.HasPrefix(rest, "--"):
		return lineEnd(query, pos)
	case strings.HasPrefix(rest, "/*"):
		if end := strings.Index(rest[len("/*"):], "*/"); end >= 0 {
			return pos + len("/*") + end + len("*/")
		}

		return len(query)
	case rest[0] == '$':
		return dollarQuoted(query, pos)
	default:
		return pos
	}
}

// closeQuote returns the index after the quote that closes a literal whose
// content starts at pos; a doubled quote is part of it, and so is a quote
// after a backslash in an E'…' string.
func closeQuote(query string, pos int, quote byte, backslash bool) int {
	for pos < len(query) {
		switch {
		case backslash && query[pos] == '\\':
			pos += len(`\'`)
		case query[pos] != quote:
			pos++
		case pos+1 < len(query) && query[pos+1] == quote:
			pos += len("''")
		default:
			return pos + 1
		}
	}

	return len(query)
}

// isEscapeString reports whether the quote at pos opens an E'…' string.
func isEscapeString(query string, pos int) bool {
	if pos == 0 || (query[pos-1] != 'E' && query[pos-1] != 'e') {
		return false
	}

	return pos == 1 || !isIdentByte(query[pos-2])
}

// lineEnd returns the index after the end of the line that holds pos.
func lineEnd(query string, pos int) int {
	if end := strings.IndexByte(query[pos:], '\n'); end >= 0 {
		return pos + end + 1
	}

	return len(query)
}

// dollarQuoted returns the end of a $tag$…$tag$ string starting at pos, or
// pos when the "$" there opens none (a "$1" placeholder, a "$" inside an
// identifier).
func dollarQuoted(query string, pos int) int {
	tagEnd := pos + 1

	switch {
	case pos > 0 && isIdentByte(query[pos-1]):
		return pos
	case tagEnd < len(query) && isDigit(query[tagEnd]):
		return pos
	}

	for tagEnd < len(query) && isIdentByte(query[tagEnd]) {
		tagEnd++
	}

	if tagEnd >= len(query) || query[tagEnd] != '$' {
		return pos
	}

	tag := query[pos : tagEnd+1]
	if end := strings.Index(query[tagEnd+1:], tag); end >= 0 {
		return tagEnd + 1 + end + len(tag)
	}

	return len(query)
}

func isIdentByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || isDigit(b) || b >= utf8.RuneSelf
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

type questionFormat struct{}

// ReplacePlaceholders implements PlaceholderFormat.
func (questionFormat) ReplacePlaceholders(query string) (string, error) { return query, nil }
