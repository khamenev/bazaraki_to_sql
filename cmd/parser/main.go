package main

import (
	"bazaraki_to_sql/internal/api"
	"bazaraki_to_sql/internal/model"
	"bazaraki_to_sql/internal/storage"
	migrations "bazaraki_to_sql/migration"
	"log"
)

const (
	dsn = "tcp://localhost:9000?database=bazaraki&user=default"
)

func main() {

	// Run migrations
	if err := migrations.Migrate(dsn); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	page := 1
	for {
		items, next, err := api.FetchPage(page)
		if err != nil {
			log.Fatalf("Failed to fetch page %d: %v", page, err)
		}

		if err := SaveItems(items); err != nil {
			log.Fatalf("Failed to save items from page %d: %v", page, err)
		}

		if next == "" {
			log.Printf("Completed fetching and saving all items.")
			break
		}
		page++
	}
}

func SaveItems(items []model.Item) error {
	log.Printf("Saving %d items", len(items))

	store := storage.NewClickHouseStorage(dsn)

	if err := store.SaveItems(items); err != nil {
		log.Printf("Failed to save items: %v", err)
		return err
	}

	log.Printf("Successfully saved items")
	return nil
}
