package sql

import (
	"strconv"
	"strings"
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
	// as jsonb's "?" and "?|". String literals are not parsed, as in
	// squirrel.
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

	for {
		pos := strings.IndexByte(query, '?')
		if pos < 0 {
			break
		}

		out.WriteString(query[:pos])

		if strings.HasPrefix(query[pos:], "??") {
			out.WriteByte('?')

			query = query[pos+2:]

			continue
		}

		n++

		out.WriteByte('$')
		out.WriteString(strconv.Itoa(n))

		query = query[pos+1:]
	}

	out.WriteString(query)

	return out.String(), nil
}

type questionFormat struct{}

// ReplacePlaceholders implements PlaceholderFormat.
func (questionFormat) ReplacePlaceholders(query string) (string, error) { return query, nil }
