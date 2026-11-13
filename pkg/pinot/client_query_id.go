package pinot

import (
	"context"
	"strings"
)

// ClientQueryIdOption is Pinot's built-in query option for tagging a query with a caller-supplied id.
// The broker echoes it back as clientRequestId, which StarTree's system_query_log stores in its own column.
const ClientQueryIdOption = "clientQueryId"

type clientQueryIdKey struct{}

// WithClientQueryId returns a context whose broker queries are tagged with an id from newId, called once per
// query. The broker looks running queries up by this id (e.g. to cancel one), so each call should return a
// distinct id. The id is sent as the clientQueryId query option, and as a comment at the start of the SQL,
// because the broker log records the query text but not its options.
//
// Ids must not contain ';' or '=' (the queryOptions field is split on them) or "*/" (it would end the
// comment); see ClientQueryIdSafe. A query whose id is empty or unsafe is sent untagged.
func WithClientQueryId(ctx context.Context, newId func() string) context.Context {
	return context.WithValue(ctx, clientQueryIdKey{}, newId)
}

// ClientQueryIdSafe reports whether id can be sent as a clientQueryId.
func ClientQueryIdSafe(id string) bool {
	return !strings.ContainsAny(id, ";=") && !strings.Contains(id, "*/")
}

// clientQueryIdFrom returns a new id for one query on ctx, and false if ctx has an id that can't be sent.
func clientQueryIdFrom(ctx context.Context) (string, bool) {
	newId, _ := ctx.Value(clientQueryIdKey{}).(func() string)
	if newId == nil {
		return "", true
	}
	if id := newId(); ClientQueryIdSafe(id) {
		return id, true
	}
	return "", false
}

// clientQueryIdComment goes before the query so it survives truncation of long query text in the
// broker log and the query log.
func clientQueryIdComment(id string) string {
	return "/* " + ClientQueryIdOption + ": " + id + " */\n"
}
