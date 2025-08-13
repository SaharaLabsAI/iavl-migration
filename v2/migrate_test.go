package v2

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestToShardID(t *testing.T) {
	tests := []struct {
		version int64
		shardID int64
	}{
		{1, 1},
		{500000, 1},
		{500001, 2},
		{1000000, 2},
		{1000001, 3},
		{4312305, 9},
		{0, 1},
		{-1, 1},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("version_%d", tt.version), func(t *testing.T) {
			result := ToShardID(tt.version)
			require.Equal(t, tt.shardID, result)
		})
	}
}

func TestCalculateShardRange(t *testing.T) {
	tests := []struct {
		minVersion int64
		maxVersion int64
		expected   []int64
	}{
		{1, 500000, []int64{1}},
		{1, 500001, []int64{1, 2}},
		{500001, 1000000, []int64{2}},
		{1, 1000000, []int64{1, 2}},
		{1, 4312305, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{0, 0, []int64{1}},
		{-1, -1, []int64{1}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("min_%d_max_%d", tt.minVersion, tt.maxVersion), func(t *testing.T) {
			result := calculateShardRange(tt.minVersion, tt.maxVersion)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestMigrateTreeSharding(t *testing.T) {
	// Create temporary directories
	tempDir := t.TempDir()
	oldPath := filepath.Join(tempDir, "old_tree.sqlite")
	newPath := filepath.Join(tempDir, "new_tree.sqlite")

	// Create old database with test data
	oldDB, err := sql.Open("sqlite", oldPath)
	require.NoError(t, err)
	defer oldDB.Close()

	// Create old database structure (v2 format with tree_1 table)
	_, err = oldDB.Exec(`
		CREATE TABLE tree_1 (
			version INT, sequence INT, bytes BLOB, orphaned BOOL,
			PRIMARY KEY (version, sequence)
		);
		CREATE TABLE root (
			version INT, node_version INT, node_sequence INT, bytes BLOB,
			PRIMARY KEY (version DESC)
		);
		CREATE TABLE orphan (
			version INT, sequence INT, at INT,
			PRIMARY KEY (at DESC, version, sequence)
		);
	`)
	require.NoError(t, err)

	// Insert test data with versions that span multiple shards
	testData := []struct {
		version  int64
		sequence int
		bytes    []byte
	}{
		{1, 1, []byte("data1")},       // shard 1
		{500000, 1, []byte("data2")},  // shard 1
		{500001, 1, []byte("data3")},  // shard 2
		{1000000, 1, []byte("data4")}, // shard 2
		{4312305, 1, []byte("data5")}, // shard 9
	}

	for _, data := range testData {
		_, err = oldDB.Exec("INSERT INTO tree_1 (version, sequence, bytes, orphaned) VALUES (?, ?, ?, ?)",
			data.version, data.sequence, data.bytes, false)
		require.NoError(t, err)
	}

	// Insert root data
	_, err = oldDB.Exec("INSERT INTO root (version, node_version, node_sequence, bytes) VALUES (?, ?, ?, ?)",
		4312305, 4312305, 1, []byte("root_data"))
	require.NoError(t, err)

	// Run migration
	err = migrateTree(oldPath, newPath)
	require.NoError(t, err)

	// Verify new database structure
	newDB, err := sql.Open("sqlite", newPath)
	require.NoError(t, err)
	defer newDB.Close()

	// Check that shard tables were created
	rows, err := newDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%' ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	var shardTables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		require.NoError(t, err)
		shardTables = append(shardTables, tableName)
	}

	// Should have tree_1, tree_2, and tree_9 (and potentially others in between)
	// The exact number depends on the version range calculation
	require.GreaterOrEqual(t, len(shardTables), 3)
	require.Contains(t, shardTables, "tree_1")
	require.Contains(t, shardTables, "tree_2")
	require.Contains(t, shardTables, "tree_9")

	// Verify data was migrated correctly
	for _, data := range testData {
		shardID := ToShardID(data.version)
		tableName := fmt.Sprintf("tree_%d", shardID)

		var count int
		err := newDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE version = ? AND sequence = ?", tableName),
			data.version, data.sequence).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	}

	// Verify root data was migrated
	var rootCount int
	err = newDB.QueryRow("SELECT COUNT(*) FROM root WHERE version = ?", 4312305).Scan(&rootCount)
	require.NoError(t, err)
	require.Equal(t, 1, rootCount)
}

func TestMigrateTreeShardingEmptyTable(t *testing.T) {
	// Create temporary directories
	tempDir := t.TempDir()
	oldPath := filepath.Join(tempDir, "old_tree_empty.sqlite")
	newPath := filepath.Join(tempDir, "new_tree_empty.sqlite")

	// Create old database with empty tree_1 table
	oldDB, err := sql.Open("sqlite", oldPath)
	require.NoError(t, err)
	defer oldDB.Close()

	// Create old database structure with empty tree_1 table
	_, err = oldDB.Exec(`
		CREATE TABLE tree_1 (
			version INT, sequence INT, bytes BLOB, orphaned BOOL,
			PRIMARY KEY (version, sequence)
		);
		CREATE TABLE root (
			version INT, node_version INT, node_sequence INT, bytes BLOB,
			PRIMARY KEY (version DESC)
		);
		CREATE TABLE orphan (
			version INT, sequence INT, at INT,
			PRIMARY KEY (at DESC, version, sequence)
		);
	`)
	require.NoError(t, err)

	// Run migration on empty table
	err = migrateTree(oldPath, newPath)
	require.NoError(t, err)

	// Verify new database structure
	newDB, err := sql.Open("sqlite", newPath)
	require.NoError(t, err)
	defer newDB.Close()

	// Check that only base tables were created (no shard tables for empty data)
	rows, err := newDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%' ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	var shardTables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		require.NoError(t, err)
		shardTables = append(shardTables, tableName)
	}

	// Should have no shard tables for empty data
	require.Empty(t, shardTables)

	// Check that base tables exist
	var tableCount int
	err = newDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&tableCount)
	require.NoError(t, err)
	require.Equal(t, 2, tableCount) // root and branch_orphan tables
}

func TestMigrateTreeShardingOnlyRootData(t *testing.T) {
	// Create temporary directories
	tempDir := t.TempDir()
	oldPath := filepath.Join(tempDir, "old_tree_root_only.sqlite")
	newPath := filepath.Join(tempDir, "new_tree_root_only.sqlite")

	// Create old database with only root data (no tree_1 data)
	oldDB, err := sql.Open("sqlite", oldPath)
	require.NoError(t, err)
	defer oldDB.Close()

	// Create old database structure with empty tree_1 but root data
	_, err = oldDB.Exec(`
		CREATE TABLE tree_1 (
			version INT, sequence INT, bytes BLOB, orphaned BOOL,
			PRIMARY KEY (version, sequence)
		);
		CREATE TABLE root (
			version INT, node_version INT, node_sequence INT, bytes BLOB,
			PRIMARY KEY (version DESC)
		);
		CREATE TABLE orphan (
			version INT, sequence INT, at INT,
			PRIMARY KEY (at DESC, version, sequence)
		);
	`)
	require.NoError(t, err)

	// Insert only root data (no tree_1 data)
	_, err = oldDB.Exec("INSERT INTO root (version, node_version, node_sequence, bytes) VALUES (?, ?, ?, ?)",
		1, 1, 1, []byte("root_data_1"))
	require.NoError(t, err)

	_, err = oldDB.Exec("INSERT INTO root (version, node_version, node_sequence, bytes) VALUES (?, ?, ?, ?)",
		2, 2, 1, []byte("root_data_2"))
	require.NoError(t, err)

	// Run migration
	err = migrateTree(oldPath, newPath)
	require.NoError(t, err)

	// Verify new database structure
	newDB, err := sql.Open("sqlite", newPath)
	require.NoError(t, err)
	defer newDB.Close()

	// Check that no shard tables were created (since tree_1 was empty)
	rows, err := newDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%' ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	var shardTables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		require.NoError(t, err)
		shardTables = append(shardTables, tableName)
	}

	// Should have no shard tables since tree_1 was empty
	require.Empty(t, shardTables)

	// Verify root data was migrated correctly
	var rootCount int
	err = newDB.QueryRow("SELECT COUNT(*) FROM root").Scan(&rootCount)
	require.NoError(t, err)
	require.Equal(t, 2, rootCount)

	// Verify specific root data
	var version1Count, version2Count int
	err = newDB.QueryRow("SELECT COUNT(*) FROM root WHERE version = ?", 1).Scan(&version1Count)
	require.NoError(t, err)
	require.Equal(t, 1, version1Count)

	err = newDB.QueryRow("SELECT COUNT(*) FROM root WHERE version = ?", 2).Scan(&version2Count)
	require.NoError(t, err)
	require.Equal(t, 1, version2Count)
}
