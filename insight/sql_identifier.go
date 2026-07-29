package insight

import (
	"fmt"
	"regexp"
)

// sqlIdentifierPattern matches a plain SQL identifier: a leading letter or
// underscore followed by letters, digits, underscores, or dollar signs. An
// optional schema qualifier of the same shape may precede the table name.
var sqlIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$`)

// validateTableName rejects table names that are not plain, optionally
// schema-qualified SQL identifiers. Data API parameters cannot bind
// identifiers, so the table name is embedded in the statement text; this
// validation guarantees the embedded value cannot alter the statement's
// structure.
func validateTableName(table string) error {
	if !sqlIdentifierPattern.MatchString(table) {
		return fmt.Errorf("invalid table name %q: must be a plain identifier, optionally schema-qualified", table)
	}
	return nil
}
