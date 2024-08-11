package storage

import (
	"bazaraki_to_sql/internal/model"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
)

type Storage interface {
	SaveItems(items []model.Item) error
}

type ClickHouseStorage struct {
	db *sql.DB
}

func NewClickHouseStorage(dsn string) *ClickHouseStorage {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to ping ClickHouse: %v", err)
	}
	return &ClickHouseStorage{db: db}
}

func (s *ClickHouseStorage) SaveItems(items []model.Item) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
        INSERT INTO items (id, title, description, price, url, created_dt, owner_advert_count, negotiable_price, rubric, city, user_id, currency, raise_dt) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, item := range items {
		url := item.GenerateURL()
		negotiablePrice := 0
		if item.NegotiablePrice {
			negotiablePrice = 1
		}

		price, err := strconv.ParseFloat(item.Price, 64)
		if err != nil {
			log.Printf("Error converting price to float for item ID %d: %v", item.ID, err)
			continue // Skip items with unparseable prices
		}

		if _, err := stmt.Exec(
			item.ID, item.Title, item.Description, price, url,
			item.CreatedDT, item.OwnerAdvertCount, negotiablePrice, item.Rubric,
			item.City, item.UserID, item.Currency, item.RaiseDT,
		); err != nil {
			log.Printf("Failed to insert item %d: %v", item.ID, err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetExistingItemIDs takes a slice of item IDs and returns those that already exist in the database.
func (s *ClickHouseStorage) GetExistingItemIDs(itemIDs []string) ([]string, error) {
	// Dynamically build the placeholder part of the query
	placeholders := make([]string, len(itemIDs))
	for i := range itemIDs {
		placeholders[i] = "?"
	}
	placeholderStr := strings.Join(placeholders, ", ")

	query := fmt.Sprintf("SELECT id FROM items WHERE id IN (%s)", placeholderStr)

	// Convert itemIDs from []string to []interface{} for the query
	args := make([]interface{}, len(itemIDs))
	for i, id := range itemIDs {
		args[i] = id
	}

	// Execute the query
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}
	defer rows.Close()

	var existingIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		existingIDs = append(existingIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through rows: %w", err)
	}

	return existingIDs, nil
}
