package socket

import (
    "strings"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

// sanitizeProtoText replaces control and line-breaking characters in protocol-visible
// text with a single space and trims the result. This prevents CR/LF injection
// and frame splitting in the line-oriented protocol.
func sanitizeProtoText(s string) string {
    if s == "" {
        return ""
    }
    // Build a sanitized rune slice.
    out := make([]rune, 0, len(s))
    for _, r := range s {
        switch {
        // Common line-breaking controls
        case r == '\r' || r == '\n' || r == '\u0085' || r == '\u2028' || r == '\u2029':
            out = append(out, ' ')
        // ASCII control chars and DEL
        case r < 0x20 || r == 0x7f:
            out = append(out, ' ')
        default:
            out = append(out, r)
        }
    }
    return strings.TrimSpace(string(out))
}

// formatCommandError maps an error to protocol ErrorCode and a user-friendly message.
// It enriches permission errors with the active principal when provided.
func formatCommandError(err error, principal string) (ErrorCode, string) {
    if err == nil {
        return OK, ""
    }
    // gRPC status handling
    if st, ok := status.FromError(err); ok {
        // Sanitize inputs used in protocol message construction
        sp := sanitizeProtoText(principal)
        sm := sanitizeProtoText(st.Message())
        switch st.Code() {
        case codes.PermissionDenied:
            // Include principal context if available
            if sp != "" {
                return Warning, "permission denied for principal '" + sp + "': " + sm
            }
            return Warning, "permission denied: " + sm
        case codes.Unauthenticated:
            return Warning, "unauthenticated: " + sm
        case codes.NotFound:
            return Warning, "not found: " + sm
        case codes.DeadlineExceeded:
            return Warning, "timeout: " + sm
        case codes.Unavailable:
            return Warning, "service unavailable: " + sm
        default:
            return Warning, sm
        }
    }
    return Warning, sanitizeProtoText(err.Error())
}
