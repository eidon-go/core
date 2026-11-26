package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// toTable copies fields into an amqp.Table, with room for extra more keys.
// amqp091 accepts nested tables only as amqp.Table, so nested
// map[string]any values — also inside []any — are converted the same way;
// the other values are copied as they are (amqp091 validates them). fields
// is not changed. Nil fields with no extra room give a nil table.
func toTable(fields map[string]any, extra int) amqp.Table {
	if fields == nil && extra == 0 {
		return nil
	}

	table := make(amqp.Table, len(fields)+extra)
	for k, v := range fields {
		table[k] = toField(v)
	}

	return table
}

// toField converts the nested maps of a header or argument value.
func toField(value any) any {
	switch nested := value.(type) {
	case map[string]any:
		return toTable(nested, 0)
	case amqp.Table:
		return toTable(nested, 0)
	case []any:
		out := make([]any, len(nested))
		for i, e := range nested {
			out[i] = toField(e)
		}

		return out
	default:
		return value
	}
}
