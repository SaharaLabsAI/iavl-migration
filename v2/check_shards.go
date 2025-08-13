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

func CheckShardsCommand() *cobra.Command {
	var (
		dbPath string
	)

	cmd := &cobra.Command{
		Use:   "check-shards",
		Short: "check shard tables in database",
		Run: func(cmd *cobra.Command, args []string) {
			checkShards(dbPath)
		},
	}

	cmd.Flags().StringVar(&dbPath, "db-path", "", "Path to the database directory")
	if err := cmd.MarkFlagRequired("db-path"); err != nil {
		panic(err)
	}

	return cmd
}

func checkShards(dbPath string) {
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

			fmt.Printf("\n=== Checking tree.sqlite: %s ===\n", path)
			if err := checkShardsInFile(path); err != nil {
				log.Printf("Error checking %s: %v", path, err)
				continue
			}
		}
		return nil
	}

	if err := walkDir(dbPath); err != nil {
		log.Fatal(err)
	}
}

func checkShardsInFile(dbPath string) error {
	// Open the database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open db %s: %w", dbPath, err)
	}
	defer db.Close()

	// Check what shard tables exist
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%' ORDER BY name")
	if err != nil {
		return fmt.Errorf("failed to query shard tables: %w", err)
	}
	defer rows.Close()

	var existingShards []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		existingShards = append(existingShards, tableName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating shard tables: %w", err)
	}

	fmt.Printf("Database: %s\n", dbPath)
	fmt.Printf("Existing shard tables: %v\n", existingShards)

	// Analyze version range to understand data distribution
	fmt.Printf("Analyzing version range...\n")

	// Get min and max versions from the root table
	var minVersion, maxVersion int64
	err = db.QueryRow("SELECT MIN(version), MAX(version) FROM root").Scan(&minVersion, &maxVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Printf("No data found in root table\n")
			return nil
		}
		return fmt.Errorf("failed to query version range: %w", err)
	}

	fmt.Printf("Version range: %d to %d\n", minVersion, maxVersion)

	// Calculate expected shard range
	expectedShards := calculateShardRange(minVersion, maxVersion)
	fmt.Printf("Expected shards based on version range: %v\n", expectedShards)

	// Check for missing shards
	existingShardMap := make(map[string]bool)
	for _, shard := range existingShards {
		existingShardMap[shard] = true
	}

	var missingShards []string
	for _, shardID := range expectedShards {
		tableName := fmt.Sprintf("tree_%d", shardID)
		if !existingShardMap[tableName] {
			missingShards = append(missingShards, tableName)
		}
	}

	if len(missingShards) > 0 {
		fmt.Printf("Missing shard tables: %v\n", missingShards)
	} else {
		fmt.Printf("All expected shard tables exist\n")
	}

	// Show data distribution across shards
	fmt.Printf("\nData distribution across shards:\n")
	for _, shard := range existingShards {
		var count int64
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", shard)).Scan(&count)
		if err != nil {
			fmt.Printf("  %s: error counting rows: %v\n", shard, err)
		} else {
			fmt.Printf("  %s: %d rows\n", shard, count)
		}
	}

	return nil
}
