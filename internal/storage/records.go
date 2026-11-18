package storage

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

type Page struct {
	Items       []map[string]any `json:"items"`
	NextCursor  int64            `json:"next_cursor"`
	HasMore     bool             `json:"has_more"`
	RunID       string           `json:"run_id"`
	SampledAtMS int64            `json:"sampled_at_ms"`
}

func Tables(role string) []string {
	common := []string{"outbox", "processed_events", "processing_attempts", "fault_injections"}
	switch role {
	case "orders":
		return append(common, "orders")
	case "payments":
		return append(common, "payment_operations")
	case "reservations":
		return append(common, "inventory", "reservation_decisions")
	case "demo-web":
		return []string{"kafka_records", "purchase_requests", "purchase_jobs", "control_operations"}
	}
	return nil
}

func Records(ctx context.Context, db *sql.DB, role, table, run, order string, after int64, limit int) (Page, error) {
	page := Page{Items: []map[string]any{}, RunID: run, NextCursor: after, SampledAtMS: time.Now().UnixMilli()}
	if !slices.Contains(Tables(role), table) {
		return page, contracts.Fail(400, "INVALID_REQUEST", "Недоступная таблица")
	}
	if after < 0 || limit < 1 || limit > 200 {
		return page, contracts.Fail(400, "INVALID_REQUEST", "Некорректный курсор или лимит")
	}
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	columns, err := columns(ctx, db, table)
	if err != nil {
		return page, err
	}
	selected := make([]string, 0, len(columns)+1)
	selected = append(selected, "rowid AS _cursor")
	for _, name := range columns {
		selected = append(selected, `"`+name+`"`)
	}
	query := "SELECT " + strings.Join(selected, ",") + " FROM " + table + " WHERE run_id=? AND rowid>?"
	args := []any{run, after}
	if order != "" && slices.Contains(columns, "order_id") {
		query += " AND order_id=?"
		args = append(args, order)
	}
	query += " ORDER BY rowid LIMIT ?"
	args = append(args, limit+1)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	names := append([]string{"_cursor"}, columns...)
	for rows.Next() {
		values := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return page, err
		}
		if len(page.Items) == limit {
			page.HasMore = true
			break
		}
		item := make(map[string]any, len(names))
		for i, name := range names {
			if b, ok := values[i].([]byte); ok {
				item[name] = string(b)
			} else {
				item[name] = values[i]
			}
		}
		cursor, ok := values[0].(int64)
		if !ok {
			return page, contracts.Protocol("invalid sqlite rowid")
		}
		page.NextCursor = cursor
		page.Items = append(page.Items, item)
	}
	return page, rows.Err()
}

func columns(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err = rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, rows.Err()
}
