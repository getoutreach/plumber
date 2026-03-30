package format

import "github.com/google/uuid"

// UUIDToString converts 16-byte UUID to human readable string
func UUIDToString(u uuid.UUID) string {
	return u.String()
}

// StringToUUID converts human readable string to 16-byte UUID
func StringToUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
