package storage

import (
	"bazaraki_to_sql/internal/model"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

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
	if err = pingWithRetries(db, 100, 2*time.Second); err != nil {
		log.Fatalf("Failed to ping ClickHouse: %v", err)
	}
	return &ClickHouseStorage{db: db}
}

// pingWithRetries pings the database with a specified number of retries.
func pingWithRetries(db *sql.DB, retries int, delay time.Duration) error {
	for i := 0; i < retries; i++ {
		if err := db.Ping(); err == nil {
			return nil
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("failed to ping database after %d attempts", retries)
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
		if err := s.insertItem(stmt, item); err != nil {
			log.Printf("Failed to insert item %d: %v", item.ID, err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// insertItem inserts a single item into the database, with retries.
func (s *ClickHouseStorage) insertItem(stmt *sql.Stmt, item model.Item) error {
	url := item.GenerateURL()
	negotiablePrice := 0
	if item.NegotiablePrice {
		negotiablePrice = 1
	}

	price, err := strconv.ParseFloat(item.Price, 64)
	if err != nil {
		log.Printf("Error converting price to float for item ID %d: %v", item.ID, err)
		return fmt.Errorf("invalid price for item ID %d", item.ID)
	}

	// Retry logic for inserting the item
	for i := 0; i < 3; i++ {
		if _, err := stmt.Exec(
			item.ID, item.Title, item.Description, price, url,
			item.CreatedDT, item.OwnerAdvertCount, negotiablePrice, item.Rubric,
			item.City, item.UserID, item.Currency, item.RaiseDT,
		); err == nil {
			return nil
		}
		time.Sleep(2 * time.Second) // Wait before retrying
	}
	return fmt.Errorf("failed to insert item %d after retries", item.ID)
}

// GetExistingItemIDs takes a slice of item IDs and returns those that already exist in the database.
func (s *ClickHouseStorage) GetExistingItemIDs(itemIDs []string) ([]string, error) {
	placeholders := make([]string, len(itemIDs))
	for i := range itemIDs {
		placeholders[i] = "?"
	}
	placeholderStr := strings.Join(placeholders, ", ")

	query := fmt.Sprintf("SELECT id FROM items WHERE id IN (%s)", placeholderStr)

	args := make([]interface{}, len(itemIDs))
	for i, id := range itemIDs {
		args[i] = id
	}

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
