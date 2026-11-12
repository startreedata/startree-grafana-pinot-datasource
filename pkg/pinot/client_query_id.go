package pinot

import (
	"context"
	"strings"
)

// ClientQueryIdOption is Pinot's built-in query option for tagging a query with a caller-supplied id.
// The broker echoes it back as clientRequestId, which StarTree's system_query_log stores in its own column.
const ClientQueryIdOption = "clientQueryId"

type clientQueryIdKey struct{}

// WithClientQueryId returns a context whose broker queries are tagged with the given id. The id is sent as
// the clientQueryId query option, and as a comment at the start of the SQL, because the broker log records
// the query text but not its options.
//
// An empty id leaves queries untagged, and so does one that can't be sent safely: the queryOptions field
// is split on ';' and '=', and "*/" would end the comment.
func WithClientQueryId(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, clientQueryIdKey{}, id)
}

func clientQueryIdFrom(ctx context.Context) string {
	id, _ := ctx.Value(clientQueryIdKey{}).(string)
	if strings.ContainsAny(id, ";=") || strings.Contains(id, "*/") {
		return ""
	}
	return id
}

// clientQueryIdComment goes before the query so it survives truncation of long query text in the
// broker log and the query log.
func clientQueryIdComment(id string) string {
	return "/* " + ClientQueryIdOption + ": " + id + " */\n"
}
