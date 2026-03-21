package extractor

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

func NormalizeKey(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func CanonicalName(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func MakeUID(label, name string) string {
	key := NormalizeKey(label) + "|" + NormalizeKey(name)
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:16])
}

func SanitizeLabel(label string) string {
	label = strings.TrimSpace(label)
	var b strings.Builder

	for _, r := range label {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		}

	}
	return b.String()
}
