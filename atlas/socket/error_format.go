package socket

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// formatCommandError maps an error to protocol ErrorCode and a user-friendly message.
// It enriches permission errors with the active principal when provided.
func formatCommandError(err error, principal string) (ErrorCode, string) {
	if err == nil {
		return OK, ""
	}
	// gRPC status handling
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.PermissionDenied:
			// Include principal context if available
			if principal != "" {
				return Warning, "permission denied for principal '" + principal + "': " + st.Message()
			}
			return Warning, "permission denied: " + st.Message()
		case codes.Unauthenticated:
			return Warning, "unauthenticated: " + st.Message()
		case codes.NotFound:
			return Warning, "not found: " + st.Message()
		case codes.DeadlineExceeded:
			return Warning, "timeout: " + st.Message()
		case codes.Unavailable:
			return Warning, "service unavailable: " + st.Message()
		default:
			return Warning, st.Message()
		}
	}
	return Warning, err.Error()
}
