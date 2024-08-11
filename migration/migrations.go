package migrations

import (
	"database/sql"
	"log"

	_ "github.com/ClickHouse/clickhouse-go"
)

// Migrate attempts to perform database migrations and returns an error if unsuccessful
func Migrate(dsn string) error {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return err // Return the error instead of logging and continuing
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return err // Ensure the database connection is alive
	}

	// Your migration logic here, for example:
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS items (
			id Int64,
			title String,
			description String,
			price Float64
		) ENGINE = MergeTree() ORDER BY id
	`)
	if err != nil {
		return err // Return the error encountered during table creation
	}

	// List of ALTER TABLE statements to add new columns
	alterStatements := []string{
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS url String",
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS created_dt String",
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS owner_advert_count Int32",
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS negotiable_price UInt8",
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS rubric Int32",
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS city Int32",
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS user_id Int64",
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS currency String",
		"ALTER TABLE items ADD COLUMN IF NOT EXISTS raise_dt String",
	}

	for _, stmt := range alterStatements {
		if _, err := db.Exec(stmt); err != nil {
			log.Printf("Could not alter table items: %v", err)
			// Decide how to handle the error. Here we just log it.
			// In some cases, you might want to return the error instead.
		} else {
			log.Printf("Successfully executed: %s", stmt)
		}
	}

	log.Println("Migration completed successfully.")
	return nil // Return nil error upon successful completion
}
