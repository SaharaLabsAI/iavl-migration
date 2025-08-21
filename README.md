# IAVL v2 to v3 Migration Tool

This tool is used to migrate IAVL v2 databases to v3 format. The v3 version introduces a sharding mechanism to improve performance.

## Sharding Mechanism Overview

### Data Storage Differences: v2 vs v3

- **v2**: Branch node data is stored in the `tree_1` table (single table)
- **v3**: Branch node data is stored in sharded tables by version range, with each shard containing 500,000 versions
  - `tree_1`: versions 1-500,000
  - `tree_2`: versions 500,001-1,000,000
  - `tree_3`: versions 1,000,001-1,500,000
  - And so on...

### Shard Calculation Formula
```
shardID = (version - 1) / 500000 + 1
```

## Usage

### 1. Execute Migration

```bash
# Migrate all stores
./migrate v2 start ./migrate v2 start --iavl2-path ~/.saharad/data/iavl2

# Migrate specific stores
./migrate v2 start ./migrate v2 start --iavl2-path ~/.saharad/data/iavl2 --store-keys evm,bank
```

The migration process will:
1. Move the origin iavl2/ to iavl2.bak/
2. Create an empty iavl2 directory
3. Analyze version distribution in the original database
4. Calculate required shard tables
5. Reorganize data according to sharding logic
6. Restart chain with the new binary

### 2. Check Hash Values

```bash
./migrate v2 check-hash --old-iavl2-path /path/to/iavl2 --new-iavl2-path /path/to/iavl3 --store-key evm
```


## Migration Process Details

### 1. Version Range Analysis
The migration tool first analyzes the version range in the original database:
```sql
SELECT MIN(version), MAX(version) FROM tree_1
```

This approach avoids loading all versions into memory, which is particularly suitable for large databases.

### 2. Data Migration Order
The migration tool processes data in the following order:

1. **Root Table Data**: Always migrates root table data (if exists)
2. **Orphan Table Data**: Migrates branch orphan data
3. **Tree Data Sharding**: If tree_1 table has data, migrates according to sharding logic

### 3. Shard Calculation
Calculate required shard tables based on version range:
- Version range 1-500,000 → only needs shard 1
- Version range 1-1,000,000 → needs shards 1, 2
- Version range 1-4,312,305 → needs shards 1, 2, 3, 4, 5, 6, 7, 8, 9

### 4. Data Reorganization
Reorganize original data according to sharding logic:
```sql
-- Example: Migrate data from versions 1-500000 to tree_1
INSERT INTO tree_1(version, sequence, bytes, orphaned)
SELECT version, sequence, bytes, orphaned FROM old.tree_1
WHERE version >= 1 AND version <= 500000
```

## Important Notes

- Ensure sufficient disk space is available
- Migration process may take a long time depending on data size
