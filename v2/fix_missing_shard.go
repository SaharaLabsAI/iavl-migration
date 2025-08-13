package v2

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

func FixMissingShardCommand() *cobra.Command {
	var (
		dbPath string
	)

	cmd := &cobra.Command{
		Use:   "fix-missing-shard",
		Short: "fix missing shard tables in migrated database",
		Run: func(cmd *cobra.Command, args []string) {
			fixMissingShard(dbPath)
		},
	}

	cmd.Flags().StringVar(&dbPath, "db-path", "", "Path to the database directory")
	if err := cmd.MarkFlagRequired("db-path"); err != nil {
		panic(err)
	}

	return cmd
}

func fixMissingShard(dbPath string) {
	// Walk through all tree.sqlite files in the database directory
	var walkDir func(dir string) error
	walkDir = func(dir string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())

			if entry.IsDir() {
				// Recursively walk subdirectories
				if err := walkDir(path); err != nil {
					return err
				}
				continue
			}

			// Only process tree.sqlite files
			if entry.Name() != "tree.sqlite" {
				continue
			}

			fmt.Printf("Processing tree.sqlite: %s\n", path)
			if err := fixMissingShardInFile(path); err != nil {
				log.Printf("Error fixing %s: %v", path, err)
				continue
			}
		}
		return nil
	}

	if err := walkDir(dbPath); err != nil {
		log.Fatal(err)
	}
}

func fixMissingShardInFile(dbPath string) error {
	// Open the database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open db %s: %w", dbPath, err)
	}
	defer db.Close()

	// Check what shard tables exist
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	if err != nil {
		return fmt.Errorf("failed to query existing shard tables: %w", err)
	}
	defer rows.Close()

	existingShards := make(map[string]bool)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		existingShards[tableName] = true
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating existing shard tables: %w", err)
	}

	// Analyze version range to determine needed shards
	fmt.Printf("Analyzing version range in %s...\n", dbPath)

	// Get min and max versions from the root table to understand the data range
	var minVersion, maxVersion int64
	err = db.QueryRow("SELECT MIN(version), MAX(version) FROM root").Scan(&minVersion, &maxVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Printf("No data found in %s\n", dbPath)
			return nil
		}
		return fmt.Errorf("failed to query version range: %w", err)
	}

	fmt.Printf("Found version range: %d to %d\n", minVersion, maxVersion)

	// Calculate needed shard IDs based on version range
	neededShards := calculateShardRange(minVersion, maxVersion)
	fmt.Printf("Need shards: %v\n", neededShards)

	// Create missing shard tables
	createdCount := 0
	for _, shardID := range neededShards {
		tableName := fmt.Sprintf("tree_%d", shardID)
		if !existingShards[tableName] {
			fmt.Printf("Creating missing %s table in %s\n", tableName, dbPath)

			createStmt := fmt.Sprintf(`CREATE TABLE %s (
			  version INT, sequence INT, bytes BLOB, orphaned BOOL,
			  PRIMARY KEY (version, sequence)
			) WITHOUT ROWID;`, tableName)

			if _, err := db.Exec(createStmt); err != nil {
				return fmt.Errorf("failed to create %s table: %w", tableName, err)
			}

			fmt.Printf("Successfully created %s table in %s\n", tableName, dbPath)
			createdCount++
		} else {
			fmt.Printf("%s table already exists in %s\n", tableName, dbPath)
		}
	}

	if createdCount > 0 {
		fmt.Printf("Created %d missing shard tables in %s\n", createdCount, dbPath)
	} else {
		fmt.Printf("All necessary shard tables already exist in %s\n", dbPath)
	}

	return nil
}
