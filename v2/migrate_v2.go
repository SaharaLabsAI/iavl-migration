package v2

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"hash"
	"log"
	"os"
	"path/filepath"
	"strings"

	"runtime"
	"sync"

	hashpool "github.com/SaharaLabsAI/iavl/v2/common/pool/hash"
	nodepool3 "github.com/SaharaLabsAI/iavl/v2/common/pool/node"
	iavl3 "github.com/SaharaLabsAI/iavl/v2/db/sqlite"
	iavl2 "github.com/sahara/iavl"
	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "v2",
		Short: "migrate iavl2/ from v2 to v3 in sqlite",
	}
	cmd.AddCommand(V2toV3Command(), CheckHash())
	// cmd.AddCommand(V2toV3Command(), CheckHash(), FixMissingShardCommand(), CheckShardsCommand())
	return cmd
}

func V2toV3Command() *cobra.Command { // 2.0.2 --> 2.2.0
	// e.g.: ./migrate v2 start --old-iavl2-path ~/.saharad/data/iavl2 --new-iavl2-path ~/.saharad/data/iavl3 --concurrent true
	var (
		dbV2, dbV3   string
		storeKeysStr string
		concurrent   bool
	)

	cmd := &cobra.Command{
		Use:   "start",
		Short: "migrate iavl2/ from v2.0.2 to v2.2.0 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			var storeKeys []string
			if storeKeysStr != "" {
				storeKeys = strings.Split(storeKeysStr, ",")
			}
			return migrate(dbV2, dbV3, storeKeys, concurrent)
		},
	}
	cmd.Flags().StringVar(&dbV2, "old-iavl2-path", "", "Path to v2 iavl2/ directory")
	cmd.Flags().StringVar(&dbV3, "new-iavl2-path", "", "Path to v3 iavl3/ directory")
	cmd.Flags().StringVar(&storeKeysStr, "store-keys", "", "Comma-separated list of store keys to migrate (default: all)")
	cmd.Flags().BoolVar(&concurrent, "concurrent", false, "Enable concurrent migration of stores (default: false)")
	cmd.MarkFlagRequired("old-iavl2-path")
	cmd.MarkFlagRequired("new-iavl2-path")
	return cmd
}

func migrate(baseOld, baseNew string, storeKeys []string, concurrent bool) error {
	stores, err := getStoreKeys(baseOld, storeKeys)
	if err != nil {
		return err
	}
	log.Printf("stores to migrate: %v", stores)
	if !concurrent {
		for _, store := range stores {
			if err := migrateStore(store, baseOld, baseNew); err != nil {
				return err
			}
		}
		return nil
	}

	maxWorkers := runtime.NumCPU()
	log.Printf("migrate concurrently, max workers %d", maxWorkers)
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	for _, store := range stores {
		wg.Add(1)
		sem <- struct{}{}

		go func(store string) {
			defer wg.Done()
			if err := migrateStore(store, baseOld, baseNew); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
			<-sem
		}(store)
	}
	wg.Wait()
	return firstErr
}

func migrateStore(store, baseOld, baseNew string) error {
	oldTreePath := filepath.Join(baseOld, store, "tree.sqlite")
	newTreePath := filepath.Join(baseNew, store, "tree.sqlite")
	oldChangelogPath := filepath.Join(baseOld, store, "changelog.sqlite")
	newChangelogPath := filepath.Join(baseNew, store, "changelog.sqlite")

	log.Printf("Processing tree.sqlite:  %s", oldTreePath)
	if _, err := os.Stat(oldTreePath); err == nil {
		if err := migrateTree(oldTreePath, newTreePath); err != nil {
			log.Printf("migrate tree.sqlite failed: %s, store: %s", err.Error(), store)
			return err
		}
	} else {
		errMsg := fmt.Sprintf("tree.sqlite not found: %s", oldTreePath)
		log.Printf(errMsg)
		return errors.New(errMsg)
	}
	log.Printf("migrate tree.sqlite successfully, store: %s", store)

	log.Printf("Processing changelog.sqlite:  %s", oldChangelogPath)
	if _, err := os.Stat(oldChangelogPath); err == nil {
		if err := migrateChangelog(oldChangelogPath, newChangelogPath); err != nil {
			log.Printf("migrate changelog.sqlite failed: %s, store: %s", err.Error(), store)
			return err
		}
	} else {
		errMsg := fmt.Sprintf("changelog.sqlite not found: %s", oldChangelogPath)
		log.Printf(errMsg)
		return errors.New(errMsg)
	}
	log.Printf("migrate changelog.sqlite successfully, store: %s", store)

	return nil
}

func migrateTree(oldPath, newPath string) error {
	// Open old db
	oldDB, err := sql.Open("sqlite", oldPath)
	if err != nil {
		return fmt.Errorf("open old db %s: %w", oldPath, err)
	}
	defer oldDB.Close()

	// Create target dir
	os.Remove(newPath)
	if err := os.MkdirAll(filepath.Dir(newPath), 0o777); err != nil {
		return err
	}
	newDB, err := sql.Open("sqlite", newPath)
	if err != nil {
		return fmt.Errorf("open new db %s: %w", newPath, err)
	}
	defer newDB.Close()

	exec := func(sqlStmt string) {
		if _, err := newDB.Exec(sqlStmt); err != nil {
			log.Fatalf("exec [%s]: %v", sqlStmt, err)
		}
	}

	// Create base tables
	exec(`CREATE TABLE branch_orphan (
	  version INT, sequence INT, at INT,
	  PRIMARY KEY (at DESC, version, sequence)
	) WITHOUT ROWID;`)
	exec(`CREATE TABLE root (
	  version INT, node_version INT, node_sequence INT, bytes BLOB,
	  PRIMARY KEY (version DESC)
	) WITHOUT ROWID;`)

	// ATTACH old db
	exec(fmt.Sprintf(`ATTACH DATABASE '%s' AS old;`, oldPath))

	// Analyze version range in the old database to determine needed shards
	log.Printf("analyzing version range in old database...")

	// First check if there's any data in the tree_1 table
	var count int64
	err = oldDB.QueryRow("SELECT COUNT(*) FROM tree_1").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to count rows in tree_1: %w", err)
	}

	// Check if there's any data in the root table
	var rootCount int64
	err = oldDB.QueryRow("SELECT COUNT(*) FROM root").Scan(&rootCount)
	if err != nil {
		return fmt.Errorf("failed to count rows in root: %w", err)
	}

	if count == 0 && rootCount == 0 {
		log.Printf("no data found in tree_1 or root tables")
		exec(`DETACH DATABASE old;`)
		return nil
	}

	// Migrate root table data first (always migrate if it exists)
	if rootCount > 0 {
		log.Printf("migrating tree: table root %s → %s\n", oldPath, newPath)
		exec(`INSERT INTO root(version, node_version, node_sequence, bytes)
		      SELECT version, node_version, node_sequence, bytes FROM old.root;`)
	}

	// Migrate orphan table data if it exists
	log.Printf("migrating tree: table branch_orphan %s → %s\n", oldPath, newPath)
	exec(`INSERT INTO branch_orphan(version, sequence, at)
	      SELECT version, sequence, at FROM old.orphan;`)

	// Only process tree_1 data if it exists
	if count > 0 {
		// Get min and max versions from the old tree_1 table (v2 format), handling NULL values
		var minVersion, maxVersion sql.NullInt64
		err = oldDB.QueryRow("SELECT MIN(version), MAX(version) FROM tree_1 WHERE version IS NOT NULL").Scan(&minVersion, &maxVersion)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Printf("no valid version data found in old database")
				exec(`DETACH DATABASE old;`)
				return nil
			}
			return fmt.Errorf("failed to query version range from tree_1: %w", err)
		}

		// Check if we got valid version data
		if !minVersion.Valid || !maxVersion.Valid {
			log.Printf("no valid version data found in tree_1 table")
			exec(`DETACH DATABASE old;`)
			return nil
		}

		log.Printf("found version range: %d to %d", minVersion.Int64, maxVersion.Int64)

		// Calculate needed shard IDs based on version range
		shardIDs := calculateShardRange(minVersion.Int64, maxVersion.Int64)
		log.Printf("need to create shards: %v", shardIDs)

		// Create all needed shard tables
		for _, shardID := range shardIDs {
			tableName := fmt.Sprintf("tree_%d", shardID)
			log.Printf("creating shard table: %s", tableName)
			exec(fmt.Sprintf(`CREATE TABLE %s (
			  version INT, sequence INT, bytes BLOB, orphaned BOOL,
			  PRIMARY KEY (version, sequence)
			) WITHOUT ROWID;`, tableName))
		}

		// Migrate tree data to appropriate shards
		log.Printf("migrating tree data to shards...")

		// For each shard, insert data for versions that belong to that shard
		for _, shardID := range shardIDs {
			tableName := fmt.Sprintf("tree_%d", shardID)

			// Calculate version range for this shard
			startVersion := (shardID-1)*500000 + 1
			endVersion := shardID * 500000

			log.Printf("migrating shard %d (versions %d-%d) to %s", shardID, startVersion, endVersion, tableName)

			// Insert data for this shard's version range from old.tree_1
			exec(fmt.Sprintf(`INSERT INTO %s(version, sequence, bytes, orphaned)
			      SELECT version, sequence, bytes, orphaned FROM (
			        SELECT version, sequence, bytes, orphaned,
			               ROW_NUMBER() OVER (PARTITION BY version, sequence ORDER BY rowid) as rn
			        FROM old.tree_1
			        WHERE version >= %d AND version <= %d
			      ) WHERE rn = 1;`, tableName, startVersion, endVersion))
		}
	} else {
		log.Printf("tree_1 table is empty, skipping tree data migration")
	}

	// DETACH
	exec(`DETACH DATABASE old;`)

	log.Printf("finish migrating tree: %s → %s\n", oldPath, newPath)
	return nil
}

// calculateShardRange calculates the range of shard IDs needed for a given version range
func calculateShardRange(minVersion, maxVersion int64) []int64 {
	if minVersion <= 0 || maxVersion <= 0 {
		return []int64{1}
	}

	minShard := ToShardID(minVersion)
	maxShard := ToShardID(maxVersion)

	var shards []int64
	for shardID := minShard; shardID <= maxShard; shardID++ {
		shards = append(shards, shardID)
	}

	return shards
}

// ToShardID calculates the shard ID for a given version
func ToShardID(version int64) int64 {
	const defaultStartShardID = int64(1)
	const defaultTreeShardSize = 500_000

	if version <= 0 {
		return defaultStartShardID
	}
	return (version-1)/defaultTreeShardSize + defaultStartShardID
}

func migrateChangelog(oldPath, newPath string) error {
	log.Printf("migrating changelog: table leaf %s → %s\n", oldPath, newPath)
	oldDB, err := sql.Open("sqlite", oldPath)
	if err != nil {
		return fmt.Errorf("open old changelog db %s: %w", oldPath, err)
	}
	defer oldDB.Close()

	// create target dir
	os.Remove(newPath)
	if err := os.MkdirAll(filepath.Dir(newPath), 0o777); err != nil {
		return err
	}

	newDB, err := sql.Open("sqlite", newPath)
	if err != nil {
		return fmt.Errorf("open new changelog db %s: %w", newPath, err)
	}
	defer newDB.Close()

	tx, err := newDB.Begin()
	if err != nil {
		return err
	}

	// create tables
	createStmt := []string{
		`CREATE TABLE leaf (
			version INT,
			sequence INT,
			key_hash BLOB,
			bytes BLOB,
			orphaned BOOL,
			PRIMARY KEY (key_hash, version DESC)
		);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence);`,
		`CREATE TABLE leaf_orphan (
			version INT,
			sequence INT,
			at INT,
			PRIMARY KEY (at DESC, version, sequence)
		) WITHOUT ROWID;`,
	}
	for _, stmt := range createStmt {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("exec %s: %w", stmt, err)
		}
	}

	// read from old table
	rows, err := oldDB.Query(`SELECT version, sequence, key, bytes FROM leaf`)

	if err != nil {
		return fmt.Errorf("read old leaf: %w", err)
	}
	defer rows.Close()

	insertStmt, err := tx.Prepare(`INSERT INTO leaf(version, sequence, key_hash, bytes) VALUES (?, ?, ?, ?)`)

	if err != nil {
		return err
	}
	defer insertStmt.Close()

	h := hashpool.Blake3Pool.Get().(hash.Hash)
	defer hashpool.Blake3Pool.Put(h)

	for rows.Next() {
		var (
			version, sequence int
			key, value        []byte
			// orphaned          bool
		)
		if err := rows.Scan(&version, &sequence, &key, &value); err != nil {
			return err
		}

		// calculate key_hash
		h.Reset()
		h.Write(key)
		keyHash := h.Sum(nil)

		if _, err := insertStmt.Exec(version, sequence, keyHash[:], value); err != nil {
			return err
		}
	}

	log.Printf("migrating changelog: table leaf_orphan %s → %s\n", oldPath, newPath)

	// ATTACH old db
	if _, err := tx.Exec(fmt.Sprintf(`ATTACH DATABASE '%s' AS old;`, oldPath)); err != nil {
		return fmt.Errorf("failed to attach old database: %w", err)
	}

	if _, err := tx.Exec(`INSERT INTO leaf_orphan(version, sequence, at)
		SELECT version, sequence, at FROM old.leaf_orphan;`); err != nil {
		return fmt.Errorf("migrate leaf_orphan: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	// DETACH
	if _, err := newDB.Exec(`DETACH DATABASE old;`); err != nil {
		return fmt.Errorf("failed to detach old database: %w", err)
	}
	log.Printf("finish migrating changelog: %s → %s\n", oldPath, newPath)

	return nil
}

func getStoreKeys(baseOld string, filter []string) ([]string, error) {
	entries, err := os.ReadDir(baseOld)
	if err != nil {
		return nil, err
	}
	var stores []string
	filterSet := make(map[string]bool)
	for _, k := range filter {
		filterSet[k] = true
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if len(filterSet) > 0 && !filterSet[entry.Name()] {
			continue
		}
		stores = append(stores, entry.Name())
	}
	return stores, nil
}

func CheckHash() *cobra.Command {
	var (
		dbv2 string
		dbv3 string
		sk   string
	)

	cmd := &cobra.Command{
		Use:   "check-hash",
		Short: "check tree root hash between old tree and migrated new tree",
		Run: func(cmd *cobra.Command, args []string) {

			v2sql, err := iavl2.NewSqliteDb(iavl2.NewNodePool(), iavl2.DefaultSqliteDbOptions(iavl2.SqliteDbOptions{Path: fmt.Sprintf("%s/%s", dbv2, sk)}))
			if err != nil {
				panic(err)
			}
			v2version, err := v2sql.LatestVersion()
			if err != nil {
				panic(err)
			}
			fmt.Println("v2 path: ", fmt.Sprintf("%s/%s", dbv2, sk), "version: ", v2version)
			v2root, err := v2sql.LoadRoot(v2version)
			if err != nil {
				panic(err)
			}
			v2hash := v2root.GetHash()
			fmt.Printf("v2 root hash: %x \n", v2hash)

			v3sql, err := iavl3.NewDB(iavl3.Options{
				Path:    fmt.Sprintf("%s/%s", dbv3, sk),
				WalSize: 1024 * 1024 * 1024,
			})
			if err != nil {
				panic(err)
			}
			v3version, err := v3sql.LatestVersion()
			if err != nil {
				panic(err)
			}
			if v2version != v3version {
				panic("version not match")
			}

			v3root, err := v3sql.LoadRoot(nodepool3.NewNodePool(), v3version)
			if err != nil {
				panic(err)
			}
			v3hash := v3root.Hash()

			if !bytes.Equal(v2hash, v3hash) {
				panic("hash not match")
			}
			log.Printf("check finished, latest version %d, root hash %x", v2version, v2hash)
		},
	}

	cmd.Flags().StringVar(&dbv2, "old-iavl2-path", "", "Path to the v2 root directory")
	cmd.Flags().StringVar(&dbv3, "new-iavl2-path", "", "Path to the v3 root directory")
	cmd.Flags().StringVar(&sk, "store-key", "", "The store which is going to be checked")
	if err := cmd.MarkFlagRequired("old-iavl2-path"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("new-iavl2-path"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("store-key"); err != nil {
		panic(err)
	}

	return cmd
}
