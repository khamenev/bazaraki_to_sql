package main

import (
	"bazaraki_to_sql/internal/api"
	"bazaraki_to_sql/internal/model"
	"bazaraki_to_sql/internal/storage"
	"bazaraki_to_sql/migration"
	"log"
	"strconv"
)

const (
	dsn = "tcp://localhost:9000?database=bazaraki&user=default"
)

func main() {

	// Run migrations
	if err := migrations.Migrate(dsn); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	var urls = []string{
		//"https://www.bazaraki.com/api/items/?rubric=681&city=12",  //houses rent
		//"https://www.bazaraki.com/api/items/?rubric=3529&city=12", //app rent
		"https://www.bazaraki.com/api/items/?rubric=3528", //app sell
		"https://www.bazaraki.com/api/items/?rubric=678",  //houses sell
		"https://www.bazaraki.com/api/items/?rubric=141",  //land
	}

	for _, url := range urls {
		page := 1
		for {
			items, next, err := api.FetchPage(url, page)
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
}

func SaveItems(items []model.Item) error {
	log.Printf("Saving %d items", len(items))

	store := storage.NewClickHouseStorage(dsn)

	// Extract item IDs
	var itemIDs []string
	for _, item := range items {
		itemIDs = append(itemIDs, strconv.Itoa(item.ID))
	}

	// Get existing item IDs from storage
	existingIDs, err := store.GetExistingItemIDs(itemIDs)
	if err != nil {
		log.Printf("Failed to check existing items: %v", err)
		return err
	}

	// Filter out items that already exist
	var newItems []model.Item
	for _, item := range items {
		if !contains(existingIDs, strconv.Itoa(item.ID)) {
			newItems = append(newItems, item)
		}
	}

	if len(newItems) == 0 {
		log.Println("No new items to save")
		return nil
	}

	if err := store.SaveItems(newItems); err != nil {
		log.Printf("Failed to save items: %v", err)
		return err
	}

	log.Printf("Successfully saved %d new items", len(newItems))
	return nil
}

// Helper function to check if slice contains a string
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
