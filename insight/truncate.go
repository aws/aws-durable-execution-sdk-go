package insight

// truncateContent truncates value to maxLen, returning a ContentField
// with Truncated=true if the value was shortened. Uses JSON-aware
// truncation when the value appears to be JSON, otherwise truncates at
// the byte boundary.
func truncateContent(value string, maxLen int) ContentField {
	if maxLen <= 0 || len(value) <= maxLen {
		return ContentField{Value: value}
	}

	truncated := truncateJSON(value, maxLen)
	return ContentField{Value: truncated, Truncated: true}
}

// truncateJSON performs JSON-aware truncation that tries to cut at a
// safe boundary (after a complete key-value pair) rather than mid-token.
// If the value doesn't look like JSON, it falls back to a simple byte
// truncation.
func truncateJSON(value string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	if len(value) <= maxLen {
		return value
	}

	// If it doesn't look like JSON, do a simple truncation.
	if len(value) == 0 || (value[0] != '{' && value[0] != '[') {
		return value[:maxLen]
	}

	// Try to find a good cut point by looking for the last complete
	// JSON boundary character (comma, closing brace/bracket) within
	// the allowed length.
	cutPoint := findJSONCutPoint(value, maxLen)
	if cutPoint > 0 {
		return value[:cutPoint]
	}

	return value[:maxLen]
}

// findJSONCutPoint scans backwards from maxLen to find the last position
// that ends a complete JSON value (after a comma, or at a closing
// brace/bracket). Returns 0 if no good cut point is found within a
// reasonable distance from maxLen.
func findJSONCutPoint(value string, maxLen int) int {
	// Don't search too far back — if we can't find a boundary within
	// 25% of maxLen, just cut at maxLen.
	minPos := maxLen - maxLen/4
	if minPos < 1 {
		minPos = 1
	}

	inString := false
	escaped := false
	lastGoodPos := 0

	for i := 0; i < maxLen && i < len(value); i++ {
		ch := value[i]

		if escaped {
			escaped = false
			continue
		}

		if ch == '\\' && inString {
			escaped = true
			continue
		}

		if ch == '"' {
			inString = !inString
			continue
		}

		if inString {
			continue
		}

		// Track positions after commas or closing delimiters as good
		// cut points.
		switch ch {
		case ',':
			pos := i + 1
			if pos >= minPos && pos <= maxLen {
				lastGoodPos = pos
			}
		case '}', ']':
			pos := i + 1
			if pos >= minPos && pos <= maxLen {
				lastGoodPos = pos
			}
		}
	}

	return lastGoodPos
}
