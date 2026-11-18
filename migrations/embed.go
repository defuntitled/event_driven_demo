package migrations

import "embed"

// Files contains the service-owned SQL schemas.
//
//go:embed */*.sql
var Files embed.FS
