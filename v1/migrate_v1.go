package v1

import (
	"bytes"
	"errors"

	"os"

	"fmt"
	"path/filepath"
	"strings"

	cmtdb "github.com/cometbft/cometbft-db"
	cmtos "github.com/cometbft/cometbft/libs/os"
	cmtstore "github.com/cometbft/cometbft/store"

	"cosmossdk.io/store/wrapper"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/kocubinski/costor-api/logz"
	"github.com/spf13/cobra"

	"github.com/SaharaLabsAI/iavl-migration/core"
	"github.com/SaharaLabsAI/iavl/v2/common/encoding"
	nodepool "github.com/SaharaLabsAI/iavl/v2/common/pool/node"
	"github.com/SaharaLabsAI/sahara-store/sdk/commitment/iavlv2"
	corestore "github.com/SaharaLabsAI/sahara-store/sdk/core/store"
	"github.com/SaharaLabsAI/sahara-store/sdk/metrics"
	"github.com/cosmos/iavl"

	_log "cosmossdk.io/log"
	dbm "github.com/cosmos/cosmos-db"

	itree3 "github.com/SaharaLabsAI/iavl/v2/tree"

	"github.com/SaharaLabsAI/iavl/v2/db/sqlite"

	"github.com/SaharaLabsAI/sahara-store/sdk/commitment"
	"github.com/SaharaLabsAI/sahara-store/sdk/db"
	"github.com/SaharaLabsAI/sahara-store/sdk/proof"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "v1",
		Short: "migrate application.db state from iavl v1 to iavl v2.2.0",
	}
	// cmd.AddCommand(iavl2Command(), metadataVersionCommand(), metadataLatestCommand(), printCommitInfo(), catchupChangeset(), allCommand(), applyChangeSetCommand()) //, snapshotCommand(), metadataCommand(), latestVersionCommand())
	cmd.AddCommand(allCommand(), applyChangeSetCommand())

	return cmd
}

const (
	latestVersionKey_v1 = "s/latest"
	latestVersionKey_v3 = "c/latest"

	commitInfoKeyFmt_v1 = "s/%d" // s/<version>
	commitInfoKeyFmt_v3 = "c/%d"

	goleveldbType = "goleveldb"
	pebbledbType  = "pebbledb"
)

func metadataLatestCommand() *cobra.Command {
	//  ./sahara-store-migrate v0 v45-metadata --db-v0 /Users/wenqi/.saharad/data --db-v2 /Users/wenqi/.saharad/data/application.db-v2/metadata.sqlite
	var (
		dbv0   string
		dbv2   string
		dbType string
	)
	cmd := &cobra.Command{
		Use:   "v45-metadata",
		Short: "migrate CosmosSDK v0.45 store metadata stored in application.db state to iavl v2 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			log := logz.Logger.With().Str("op", "migrate").Logger()

			// get storekeys
			v0, err := core.NewReadonlyStore(dbv0, dbType)
			if err != nil {
				return err
			}
			defer v0.Close()

			var storeKeys []string
			for k := range v0.CommitInfoByName() {
				storeKeys = append(storeKeys, k)
			}
			fmt.Printf("storekeys: %+v\n", storeKeys)

			v2, err := core.NewStore(dbv2, dbType)
			if err != nil {
				return err
			}
			defer v2.Close()

			// get and set latest version
			bz, err := v0.Get([]byte(latestVersionKey_v1))
			if err != nil {
				return err
			}

			i64 := &types.Int64Value{}
			err = proto.Unmarshal(bz, i64)
			if err != nil {
				return err
			}
			latestVersion := i64.Value
			log.Info().Msgf("latest version: %d\n", latestVersion)

			var buf bytes.Buffer
			buf.Grow(encoding.EncodeUvarintSize(uint64(latestVersion)))
			if err := encoding.EncodeUvarint(&buf, uint64(latestVersion)); err != nil {
				return err
			}

			if err = v2.Set([]byte(latestVersionKey_v3), buf.Bytes()); err != nil {
				return err
			}

			// get and set commitInfo with latestVersion
			bz, err = v0.Get([]byte(fmt.Sprintf(commitInfoKeyFmt_v1, latestVersion)))
			if err != nil {
				return err
			}
			v0CommitInfo := &CommitInfo{}
			if err = proto.Unmarshal(bz, v0CommitInfo); err != nil {
				return err
			}
			fmt.Printf("metadata v0 commitinfo: %+v\n", *v0CommitInfo)

			v3CommitInfo := ConvertCommitmentInfo(v0CommitInfo)
			v3ciBz, err := v3CommitInfo.Marshal()
			if err != nil {
				panic(err)
			}
			if err = v2.Set([]byte(fmt.Sprintf(commitInfoKeyFmt_v3, latestVersion)), v3ciBz); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&dbv0, "db-v0", "", "Path to the v0 application.db")
	cmd.Flags().StringVar(&dbv2, "db-v2", "", "Path to the v2 root")
	cmd.Flags().StringVar(&dbType, "db-type", goleveldbType, "db type of application.db")

	if err := cmd.MarkFlagRequired("db-v0"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("db-v2"); err != nil {
		panic(err)
	}
	return cmd
}

func metadataVersionCommand() *cobra.Command {
	//  ./sahara-store-migrate v0 v45-metadata --db-v0 /Users/wenqi/.saharad/data --db-v2 /Users/wenqi/.saharad/data/application.db-v2/metadata.sqlite
	var (
		v1appPath string
		// dbv2 string
		v2appPath string
		version   int64
	)
	cmd := &cobra.Command{
		Use:   "metadata-version",
		Short: "migrate CosmosSDK v0.45 store metadata stored in application.db state to iavl v2 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			log := logz.Logger.With().Str("op", "migrate").Logger()

			// get storekeys from v1 application.db
			// v0, err := core.NewReadonlyStore(dbv0)
			v0, err := core.NewStoreWithAppPath(v1appPath, core.Iavl1Type, goleveldbType)
			if err != nil {
				return err
			}
			defer v0.Close()

			var storeKeys []string
			for k := range v0.CommitInfoByName() {
				storeKeys = append(storeKeys, k)
			}
			fmt.Printf("storekeys: %+v\n", storeKeys)

			v2, err := core.NewDBWithAppPath(v2appPath, goleveldbType)
			if err != nil {
				return err
			}

			if version == 0 {
				bz, err := v0.Get([]byte(latestVersionKey_v1))
				if err != nil {
					return err
				}

				i64 := &types.Int64Value{}
				err = proto.Unmarshal(bz, i64)
				if err != nil {
					return err
				}
				version = i64.Value
				log.Info().Msgf("latest version: %d\n", i64.Value)
			}

			var buf bytes.Buffer
			buf.Grow(encoding.EncodeUvarintSize(uint64(version)))
			if err := encoding.EncodeUvarint(&buf, uint64(version)); err != nil {
				return err
			}

			if err = v2.Set([]byte(latestVersionKey_v3), buf.Bytes()); err != nil {
				return err
			}

			// get and set commitInfo with latestVersion
			bz, err := v0.Get([]byte(fmt.Sprintf(commitInfoKeyFmt_v1, version)))
			if err != nil {
				return err
			}
			v0CommitInfo := &CommitInfo{}
			if err = proto.Unmarshal(bz, v0CommitInfo); err != nil {
				return err
			}
			fmt.Printf("metadata v0 commitinfo: %+v\n", *v0CommitInfo)

			v3CommitInfo := ConvertCommitmentInfo(v0CommitInfo)
			v3ciBz, err := v3CommitInfo.Marshal()
			if err != nil {
				panic(err)
			}
			if err = v2.Set([]byte(fmt.Sprintf(commitInfoKeyFmt_v3, version)), v3ciBz); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&v1appPath, "v1-app", "", "Path to the v1 application.db")
	cmd.Flags().StringVar(&v2appPath, "v2-app", "", "Path to the v2 root")
	cmd.Flags().Int64Var(&version, "version", 0, "metadata version, default 0 means the latest version")
	if err := cmd.MarkFlagRequired("v1-app"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("v2-app"); err != nil {
		panic(err)
	}
	return cmd
}

func migrateCommitInfos(from, to int64, v1appPath, v2appPath string, dbType string) error {
	v1db, err := core.NewStoreWithAppPath(v1appPath, core.Iavl1Type, dbType)
	if err != nil {
		return err
	}
	defer v1db.Close()

	v2db, err := core.NewStoreWithAppPath(v2appPath, core.Iavl2Type, dbType)
	if err != nil {
		return err
	}
	defer v2db.Close()

	if from > to || from <= 0 {
		return errors.New("wrong version range, has to larger than 0 and from <= to")
	}

	// set commitInfos
	fmt.Printf("migrating commitInfo from %d to %d\n", from, to)
	for v := from; v <= to; v++ {
		// get and set commitInfo with latestVersion
		bz, err := v1db.Get([]byte(fmt.Sprintf(commitInfoKeyFmt_v1, v)))
		if err != nil {
			return err
		}
		v0CommitInfo := &CommitInfo{}
		if err = proto.Unmarshal(bz, v0CommitInfo); err != nil {
			return err
		}
		fmt.Printf("metadata v0 commitinfo: %+v\n", *v0CommitInfo)

		v3CommitInfo := ConvertCommitmentInfo(v0CommitInfo)
		v3ciBz, err := v3CommitInfo.Marshal()
		if err != nil {
			panic(err)
		}
		if err = v2db.Set([]byte(fmt.Sprintf(commitInfoKeyFmt_v3, v)), v3ciBz); err != nil {
			return err
		}

	}

	// set latest version
	var buf bytes.Buffer
	buf.Grow(encoding.EncodeUvarintSize(uint64(to)))
	if err := encoding.EncodeUvarint(&buf, uint64(to)); err != nil {
		return err
	}

	if err := v2db.Set([]byte(latestVersionKey_v3), buf.Bytes()); err != nil { // latestVersionKey
		return err
	}
	fmt.Printf("set metadata latestVersion %d\n", to)

	return nil
}

func printCommitInfo() *cobra.Command {
	//  ./sahara-store-migrate v0 printci --data-path /Users/wenqi/.saharad/data --iavl-version v1
	var (
		dataPath    string
		iavlVersion string
		dbType      string
	)
	cmd := &cobra.Command{
		Use:   "printci",
		Short: "",
		RunE: func(cmd *cobra.Command, args []string) error {
			// log := logz.Logger.With().Str("op", "migrate").Logger()

			var (
				appdb *core.ReadonlyStore
				err   error
			)
			switch iavlVersion {
			case "v1":
				appdb, err = core.NewReadonlyStore(dataPath, dbType)
				if err != nil {
					return err
				}
				defer appdb.Close()
			case "v3":
				appdb, err = core.NewReadonlyStoreForV3(dataPath, "v3", dbType)
				if err != nil {
					return err
				}
				defer appdb.Close()
			}

			latestValue, err := appdb.Get([]byte("c/latest"))
			if err != nil {
				panic(err)
			} else {
				fmt.Printf("latest value is %x\n", latestValue)
			}
			var storeKeys []string
			for k := range appdb.CommitInfoByName() {
				storeKeys = append(storeKeys, k)
			}
			fmt.Printf("storekeys: %+v\n", storeKeys)

			return nil
		},
	}

	cmd.Flags().StringVar(&dataPath, "data-path", "", "Path to the v0 application.db")
	cmd.Flags().StringVar(&iavlVersion, "iavl-version", "v1", "v1 or v2 or v3")
	cmd.Flags().StringVar(&dbType, "db-type", goleveldbType, "db type of application.db")
	if err := cmd.MarkFlagRequired("data-path"); err != nil {
		panic(err)
	}
	return cmd
}

func iavl2Command() *cobra.Command { // migrate one version
	// ./sahara-store-migrate v0 iavl2 --db-v0 /Users/wenqi/.saharad/data  --db-v2 /Users/wenqi/.saharad/data/iavl2
	var (
		v1appPath string
		root      string
		dbv2      string

		storekey      string
		concurrency   int
		migrateLatest bool
		dbType        string
	)
	cmd := &cobra.Command{
		Use:   "iavl2",
		Short: "migrate latest iavl v1 application.db state to iavl v2 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _, err := migrateIavl2(filepath.Join(root, "data"), v1appPath, dbv2, storekey, concurrency, migrateLatest, dbType)
			return err
		},
	}
	cmd.Flags().StringVar(&v1appPath, "v1-app", "", "Path to the v1 application.db")
	cmd.Flags().StringVar(&dbv2, "db-v2", "", "Path to the v2 root")
	cmd.Flags().StringVar(&root, "root", "", "Path to sahara root")
	cmd.Flags().StringVar(&storekey, "store-key", "", "Store key to migrate")
	cmd.Flags().BoolVar(&migrateLatest, "latest", false, "migrate the latest version data or not, default earliest version")
	cmd.Flags().StringVar(&dbType, "db-type", goleveldbType, "db type of application.db")
	if err := cmd.MarkFlagRequired("v1-app"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("db-v2"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("root"); err != nil {
		panic(err)
	}
	cmd.Flags().IntVar(&concurrency, "concurrency", 6, "Number of concurrent migrations")

	return cmd
}

func allCommand() *cobra.Command { // migrate all version include historic data
	// ./migrate v1 all --root /Users/wenqi/.saharad
	// the old cmd: ./sahara-store-migrate v0 all --v1-app /Users/wenqi/.saharad/data/application.db --v2-app /Users/wenqi/.saharad/data/application-v2.db --v2-iavl2 /Users/wenqi/.saharad/data/iavl2 --root /Users/wenqi/.saharad

	var (
		rootPath    string
		dataPath    string
		v1appPath   string
		v2appPath   string
		v2iavl2Path string
		dbType      string
		concurrency int
	)

	cmd := &cobra.Command{
		Use:   "all",
		Short: "migrate latest iavl v0 application.db state to iavl v2 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			// prepare data dirs
			dataPath = filepath.Join(rootPath, "data")
			v1appPath = filepath.Join(dataPath, "application.db")
			v2appPath = filepath.Join(dataPath, "application-v2.db")
			v2iavl2Path = filepath.Join(dataPath, "iavl2")

			if _, err := os.Stat(v1appPath); err != nil {
				return fmt.Errorf("iavl1 application.db path %s, stat err: %w", v1appPath, err)
			}

			// Check if v2appPath already exists
			if _, err := os.Stat(v2appPath); err == nil {
				return fmt.Errorf("v2 application path already exists: %s", v2appPath)
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("stat v2 application path %s: %w", v2appPath, err)
			}

			// Check if v2iavl2Path already exists
			if _, err := os.Stat(v2iavl2Path); err == nil {
				return fmt.Errorf("v2 iavl2 path already exists: %s", v2iavl2Path)
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("stat v2 iavl2 path %s: %w", v2iavl2Path, err)
			}

			// Create v2appPath directory
			if err := os.MkdirAll(v2appPath, 0o755); err != nil {
				return fmt.Errorf("create v2 application directory %s: %w", v2appPath, err)
			}

			// Create v2iavl2Path directory
			if err := os.MkdirAll(v2iavl2Path, 0o755); err != nil {
				return fmt.Errorf("create v2 iavl2 directory %s: %w", v2iavl2Path, err)
			}

			// Migrate iavl2 data on earliest height
			earliestHeight, latestHeight, err := migrateIavl2(dataPath, v1appPath, v2iavl2Path, "", concurrency, false, dbType)
			if err != nil {
				panic(err)
			}
			fmt.Printf("block height range: %d - %d\n", earliestHeight, latestHeight)

			// prepare dbs
			fmt.Println("Preparing DBs")
			v1db, err := core.NewStoreWithAppPath(v1appPath, core.Iavl1Type, dbType)
			if err != nil {
				return err
			}
			defer v1db.Close()

			v2db, err := core.NewDBWithAppPath(v2appPath, dbType)
			if err != nil {
				return err
			}
			defer v2db.Close()

			// Migrate metadata latestVersion
			// 0 means get latestVersion from v1 application.db
			var from, to int64
			appLatestVersion, err := migrateMetaLatestVersion(0, v1db, v2db)
			if err != nil {
				return err
			}
			fmt.Println("latest version from v0 application is ", appLatestVersion)

			if appLatestVersion < earliestHeight {
				return fmt.Errorf("invalid version, appLatestVersion < earliestHeight is not allowed. appLatestVersion: %d, earliestHeight: %d", appLatestVersion, earliestHeight)
			}
			if appLatestVersion < latestHeight {
				from, to = earliestHeight, appLatestVersion
			} else {
				from, to = earliestHeight, latestHeight
			}

			// Migrate metadata commitInfos
			fmt.Println("Start migrating metadata commit infos")
			if err = migrateMetaCommitInfo([]int64{from, to}, v1db, v2db); err != nil {
				return err
			}

			// Migrate historic changesets
			// commitStore.metadata base db path is root/data/application.db
			var storeKeys []string
			for k := range v1db.CommitInfoByName() {
				storeKeys = append(storeKeys, k)
			}

			fmt.Println("Preparing CommitStore")
			commitStore, err := PrepareCommitStoreWithDB(storeKeys, v2iavl2Path, earliestHeight, v2db)
			if err != nil {
				panic(err)
			}

			fmt.Println("Start applying changesets")
			if err = ApplyChangeSet(v1appPath, storeKeys, from, to, commitStore, v1db.DB, dbType); err != nil {
				panic(err)
			}

			// mv new application-v2.db to appliation.db
			if err := os.Rename(v1appPath, v1appPath+".origin"); err != nil {
				return fmt.Errorf("rename %s to %s: %w", v1appPath, v1appPath+".origin", err)
			}

			if err := os.Rename(v2appPath, v1appPath); err != nil {
				return fmt.Errorf("rename %s to %s: %w", v2appPath, v1appPath, err)
			}
			return nil
		},
	}
	// cmd.Flags().StringVar(&v2iavl2Path, "v2-iavl2", "", "Path to the iavl2/")
	// cmd.Flags().StringVar(&v1appPath, "v1-app", "", "Path to the v1 application db")
	// cmd.Flags().StringVar(&v2appPath, "v2-app", "", "Path to the v2 application db")
	cmd.Flags().StringVar(&rootPath, "root", "", "Path to the .saharad")
	cmd.Flags().StringVar(&dbType, "db-type", goleveldbType, "db type of original application.db")

	// if err := cmd.MarkFlagRequired("v1-app"); err != nil {
	// 	panic(err)
	// }
	// if err := cmd.MarkFlagRequired("v2-app"); err != nil {
	// 	panic(err)
	// }
	// if err := cmd.MarkFlagRequired("v2-iavl2"); err != nil {
	// 	panic(err)
	// }
	if err := cmd.MarkFlagRequired("root"); err != nil {
		panic(err)
	}
	cmd.Flags().IntVar(&concurrency, "concurrency", 6, "Number of concurrent migrations")

	return cmd
}

func migrateIavl2(dataPath string, v1appPath string, dbv2 string, storekey string, concurrency int, migrateLatest bool, dbType string) (int64, int64, error) {
	// 1. get store keys
	var (
		storeKeys      []string
		err            error
		migrateVersion int64
	)

	if storekey != "" {
		storeKeys = []string{storekey}
	} else {
		storeKeys, err = getStoreKeys(v1appPath, dbType)
		if err != nil {
			return 0, 0, err
		}
	}
	fmt.Println("get storeKeys:", storeKeys)

	// lock := make(chan struct{}, concurrency)
	// for i := 0; i < concurrency; i++ {
	// 	lock <- struct{}{}
	// }

	// 2. get block height range from blockStore
	earliestHeight, latestHeight, err := fetchBlockRange(dbType, dataPath) // latestHeight
	if err != nil {
		return 0, 0, err
	}
	fmt.Println("base: ", earliestHeight, "height: ", latestHeight)

	// get application latestVersion from application
	// appdb, err := core.NewDBWithAppPath(v1appPath)
	fmt.Println("migrateIavl2 v1appPath: ", v1appPath)
	appdb, err := OpenDB(v1appPath, dbType)
	if err != nil {
		return 0, 0, err
	}
	defer appdb.Close()

	bz, err := appdb.Get([]byte(latestVersionKey_v1))
	if err != nil {
		return 0, 0, err
	}

	i64 := &types.Int64Value{}
	err = proto.Unmarshal(bz, i64)
	if err != nil {
		return 0, 0, err
	}
	if i64.Value < latestHeight {
		latestHeight = i64.Value
	}

	if migrateLatest {
		fmt.Println("migrate latest true")
		// migrate the latest version data
		migrateVersion = latestHeight
	} else {
		fmt.Println("migrate latest false")
		migrateVersion = earliestHeight
	}

	fmt.Println("migrate iavl2, version: ", migrateVersion)
	// 3. prepare v0 db
	// dir := filepath.Join(dbv0, "application.db") //dbv0 + "application.db"
	// appdb, err := OpenDB(v1appPath)
	// if err != nil {
	// 	fmt.Println("panic position: OpenDB")
	// 	return earliestHeight, latestHeight, err
	// }
	// defer appdb.Close()
	fmt.Println("migrate iavl2, storeKeys: ", storeKeys)
	for _, sk := range storeKeys {
		// wg.Add(1)
		// go func(sk string) {
		var (
		// count int64
		//since = time.Now()
		)

		// <-lock

		log := logz.Logger.With().Str("store", sk).Logger()
		log.Info().Msgf("migrating %s", sk)

		prefix := fmt.Sprintf("s/k:%s/", sk)
		fmt.Println("read tree dir: ", v1appPath, "version: ", migrateVersion, "prefix: ", prefix)
		db := dbm.NewPrefixDB(appdb, []byte(prefix))

		tree := iavl.NewMutableTree(wrapper.NewDBWrapper(db), 10000, false, _log.NewLogger(os.Stdout))
		ver, _ := tree.LoadVersion(migrateVersion)
		fmt.Printf("Got version: %d\n", ver)

		v0Hash := tree.Hash()
		if len(v0Hash) == 0 {
			return earliestHeight, latestHeight, errors.New("len v0Hash is 0")
		}
		fmt.Printf("v0_tree_hash: %x\n", v0Hash)

		v2StorePath := fmt.Sprintf("%s/%s", dbv2, sk)
		fmt.Println("v2 store path: ", v2StorePath)
		sql, err := sqlite.NewDB(sqlite.Options{
			Path:    v2StorePath,
			WalSize: 1024 * 1024 * 1024,
		})
		if err != nil {
			return earliestHeight, latestHeight, err
		}
		defer sql.Close()

		// exporter, err := tree.ExportPreOrder()
		exporter, err := tree.Export()
		if err != nil {
			return earliestHeight, latestHeight, err

		}
		defer exporter.Close()

		// importer
		v3StoreTree := itree3.NewTree(sql, nodepool.NewNodePool(), itree3.DefaultOptions())
		importer, err := itree3.NewImporter(v3StoreTree, migrateVersion)
		if err != nil {
			return earliestHeight, latestHeight, err
		}
		defer importer.Close()

		for {
			n, err := exporter.Next()
			if errors.Is(err, iavl.ErrorExportDone) {
				break
			}
			if err != nil {
				fmt.Println("exporter next() error: ", err.Error())
				return earliestHeight, latestHeight, err

			}
			if n == nil {
				return earliestHeight, latestHeight, fmt.Errorf("nil node for %s", sk)
			}

			if err = importer.Add(itree3.NewImportNode(n.Key, n.Value, n.Version, n.Height)); err != nil {
				return earliestHeight, latestHeight, err

			}
		}
		if err = importer.Commit(); err != nil {
			return earliestHeight, latestHeight, err

		}
		importer.Close()

		v3hash := v3StoreTree.Hash()
		fmt.Printf("v2_tree_hash: %x\n", v3hash)

		if !bytes.Equal(v3hash, v0Hash) {
			panic(fmt.Sprintf("v2 hash=%x != v0 hash=%x", v3hash, v0Hash))
		}

		fmt.Println("finish migrating ", sk)
		// lock <- struct{}{}
		// wg.Done()
		// }(storeKey)
	}

	// wg.Wait()
	return earliestHeight, latestHeight, nil

}

// migrateMetaLatestVersion set latestVersion to v2 application.db
// if param 'version' is 0, set version with latestVersion in application.db
// return latestVersion in application.db
func migrateMetaLatestVersion(version int64, v1db, v2db *core.ReadonlyStore) (int64, error) {
	var latestVersion int64

	// get latestVersion from v0 db
	bz, err := v1db.Get([]byte(latestVersionKey_v1))
	if err != nil {
		return 0, err
	}

	i64 := &types.Int64Value{}
	err = proto.Unmarshal(bz, i64)
	if err != nil {
		return 0, err
	}
	if version == 0 {
		latestVersion = i64.Value
		// log.Info().Msgf("latest version: %d\n", latestVersionFromV0App)
	} else {
		latestVersion = version
	}

	var buf bytes.Buffer
	buf.Grow(encoding.EncodeUvarintSize(uint64(latestVersion)))
	if err := encoding.EncodeUvarint(&buf, uint64(latestVersion)); err != nil {
		return i64.Value, err
	}

	if err := v2db.Set([]byte(latestVersionKey_v3), buf.Bytes()); err != nil {
		return i64.Value, err
	}
	return i64.Value, nil
}

func migrateMetaCommitInfo(commitInfoRange []int64, v1db, v2db *core.ReadonlyStore) error {
	var (
		// migrateSingleVersion bool
		from, to int64
	)

	if commitInfoRange == nil || len(commitInfoRange) > 2 || len(commitInfoRange) == 0 {
		return errors.New("wrong parameter, length of commitInfoRange should be 1 or 2")
	} else if len(commitInfoRange) == 2 {
		from, to = commitInfoRange[0], commitInfoRange[1]
	} else { // len(commitInfoRange) == 1
		from, to = commitInfoRange[0], commitInfoRange[0]
	}
	if from > to {
		return errors.New("wrong parameter, commitInfoRange[0] can not be larger than commitInfoRange[1]")
	}

	// get and set commitInfo
	for v := from; v <= to; v++ {
		bz, err := v1db.Get([]byte(fmt.Sprintf(commitInfoKeyFmt_v1, v)))
		if err != nil {
			return err
		}
		v1CommitInfo := &CommitInfo{}
		if err = proto.Unmarshal(bz, v1CommitInfo); err != nil {
			return err
		}
		fmt.Printf("metadata v1 commitinfo: %+v\n", *v1CommitInfo)

		v3CommitInfo := ConvertCommitmentInfo(v1CommitInfo)
		v3ciBz, err := v3CommitInfo.Marshal()
		if err != nil {
			return err
		}
		if err = v2db.Set([]byte(fmt.Sprintf(commitInfoKeyFmt_v3, v)), v3ciBz); err != nil {
			return err
		}
	}

	return nil
}

func OpenDB(dir string, dbType string) (dbm.DB, error) {
	switch {
	case strings.HasSuffix(dir, ".db"):
		dir = dir[:len(dir)-3]
	case strings.HasSuffix(dir, ".db/"):
		dir = dir[:len(dir)-4]
	default:
		return nil, fmt.Errorf("database directory must end with .db")
	}

	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	// TODO: doesn't work on windows!
	cut := strings.LastIndex(dir, "/")
	if cut == -1 {
		return nil, fmt.Errorf("cannot cut paths on %s", dir)
	}
	name := dir[cut+1:]

	var db dbm.DB
	switch dbType {
	case goleveldbType:
		db, err = dbm.NewGoLevelDB(name, dir[:cut], nil)
	case pebbledbType:
		db, err = dbm.NewPebbleDB(name, dir[:cut], nil)
	}

	if err != nil {
		return nil, err
	}
	return db, nil
}

func loadBlockStore(dbBackend, dbDir string) (cmtdb.DB, error) { // goleveldb,
	var (
		blockStoreDB cmtdb.DB
		err          error
	)

	dbType := cmtdb.BackendType(dbBackend)
	fmt.Println("dbType: ", dbType, "filepath: ", filepath.Join(dbDir, "blockstore.db"))
	if !cmtos.FileExists(filepath.Join(dbDir, "blockstore.db")) {
		return nil, fmt.Errorf("no blockstore found in %v", dbDir)
	}

	// Get BlockStore
	blockStoreDB, err = cmtdb.NewDB("blockstore", cmtdb.BackendType(dbBackend), dbDir)
	if err != nil {
		return nil, err
	}

	return blockStoreDB, nil
}

func fetchBlockRange(dbBackend, dbDir string) (int64, int64, error) {
	blockStoreDB, err := loadBlockStore(dbBackend, dbDir)
	if err != nil {
		return 0, 0, err
	}
	defer blockStoreDB.Close()

	blockStore := cmtstore.NewBlockStore(blockStoreDB)
	base, latest := blockStore.Base(), blockStore.Height()

	return base, latest, nil
}

func getStoreKeys(v1appPath string, dbType string) ([]string, error) {
	rs, err := core.NewStoreWithAppPath(v1appPath, core.Iavl1Type, dbType)
	if err != nil {
		return nil, err
	}
	defer rs.DB.Close()

	var storeKeys []string
	for k := range rs.CommitInfoByName() {
		storeKeys = append(storeKeys, k)
	}

	return storeKeys, nil
}

func ConvertCommitmentInfo(v0cinfo *CommitInfo) *proof.CommitInfo {
	v2cinfo := proof.CommitInfo{
		Version:   uint64(v0cinfo.Version),
		Timestamp: v0cinfo.Timestamp,
	}

	if len(v0cinfo.StoreInfos) != 0 {
		fmt.Println("len(v0cinfo.StoreInfos) != 0")
		v2cinfo.StoreInfos = make([]*proof.StoreInfo, 0, len(v0cinfo.StoreInfos))
		for _, sf := range v0cinfo.StoreInfos {
			v2cinfo.StoreInfos = append(v2cinfo.StoreInfos, &proof.StoreInfo{
				Name: []byte(sf.Name),
				CommitID: &proof.CommitID{
					Version: uint64(sf.CommitId.Version),
					Hash:    sf.CommitId.Hash,
				},
				Structure: "",
			})
		}
	} else {
		fmt.Println("len(v0cinfo.StoreInfos) == 0")
		v2cinfo.StoreInfos = make([]*proof.StoreInfo, 0)
	}
	cihash := v2cinfo.Hash()
	fmt.Printf("v2cinfo.Hash(): %x\n", cihash)
	v2cinfo.CommitHash = cihash

	return &v2cinfo
}

func catchupChangeset() *cobra.Command { // migrate one version
	//
	var (
		v1appPath string // eg: .../.saharad/data/application.db
		v2appPath string
		rootPath  string // eg: .../.saharad
		dbv2      string // eg: .../.saharad/data/iavl2
		dbType    string
	)
	cmd := &cobra.Command{
		Use:   "catchup-changesets",
		Short: "migrate latest iavl v1 application.db state to iavl v3 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			from, to, err := fetchBlockRange(dbType, filepath.Join(rootPath, "data")) //v1dataPath)
			if err != nil {
				panic(err)
			}
			storekeys, err := getStoreKeys(v1appPath, dbType)
			if err != nil {
				panic(err)
			}

			// commitStore.metadata base db path is root/data/application.db
			commitStore, err := PrepareCommitStore(storekeys, v2appPath, dbv2)
			if err != nil {
				panic(err)
			}

			if err = ApplyChangeSet(v1appPath, storekeys, from, to, commitStore, nil, dbType); err != nil {
				panic(err)
			}

			if err = migrateCommitInfos(from, to, v1appPath, v2appPath, dbType); err != nil {
				panic(err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&v1appPath, "v1-app", "", "Path to the v1 application.db")
	cmd.Flags().StringVar(&v2appPath, "v2-app", "", "Path to the v2 application.db")
	cmd.Flags().StringVar(&rootPath, "root", "", "Path to the sahara root")
	cmd.Flags().StringVar(&dbv2, "db-v2", "", "Path to the v2 root")
	cmd.Flags().StringVar(&dbType, "db-type", goleveldbType, "db type of application.db")

	if err := cmd.MarkFlagRequired("v1-app"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("v2-app"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("root"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("db-v2"); err != nil {
		panic(err)
	}

	return cmd
}

func PrepareCommitStore(storeKeys []string, v2appPath string, iavl2Dir string) (*commitment.CommitStore, error) {
	fmt.Println("PrepareCommitStore v2appPath:", v2appPath, "iavl2Dir: ", iavl2Dir)
	appDir := filepath.Dir(v2appPath)
	appFileName := filepath.Base(v2appPath)
	appFileNameWithoutExt := strings.TrimSuffix(appFileName, filepath.Ext(".db"))

	// scRawDb used for recording metadata
	scRawDb, err := db.NewDB(
		db.DBType(goleveldbType),
		appFileNameWithoutExt,
		appDir,
		nil,
	)
	if err != nil {
		return nil, err
	}

	metadata := commitment.NewMetadataStore(scRawDb)

	latestVersion, err := metadata.GetLatestVersion()
	if err != nil {
		return nil, err
	}
	removedStoreKeys, err := metadata.GetRemovedStoreKeys(latestVersion)
	if err != nil {
		return nil, err
	}

	newTreeFn := func(key string) (commitment.CompatV1Tree, error) {
		dir := fmt.Sprintf("%s/%s", iavl2Dir, key) // .../data/iavl2/storekey
		fmt.Println("PrepareCommitStore newTreeFn dir: ", dir)
		dbOpts := sqlite.Options{Path: dir, Metrics: metrics.NoOpMetrics{}, Logger: _log.NewNopLogger()} // , OptimizeOnStart: true}

		return iavlv2.NewTree(itree3.DefaultOptions(), dbOpts, _log.NewNopLogger())
	}

	trees := make(map[string]commitment.CompatV1Tree, len(storeKeys))
	for _, key := range storeKeys {
		tree, err := newTreeFn(key)
		if err != nil {
			panic(err)
		}
		trees[key] = tree
	}

	oldTrees := make(map[string]commitment.CompatV1Tree, len(storeKeys))
	for _, key := range removedStoreKeys {
		tree, err := newTreeFn(string(key))
		if err != nil {
			return nil, err
		}
		oldTrees[string(key)] = tree
	}

	commitStore, err := commitment.NewCommitStore(trees, oldTrees, scRawDb, _log.NewNopLogger(), metrics.NoOpMetrics{})
	if err != nil {
		return nil, err
	}

	csLatestVersion, err := commitStore.GetLatestVersion()
	if err != nil {
		fmt.Println("PrepareCommitStore GetLatestVersion failed: ", err.Error())
		return nil, err
	}
	fmt.Println("PrepareCommitStore GetLatestVersion: ", csLatestVersion)

	if err = commitStore.LoadVersionForOverwriting(uint64(1)); err != nil {
		fmt.Println("PrepareCommitStore LoadVersionForOverwriting version 1, failed: ", err.Error())

		return nil, err
	}
	fmt.Println("PrepareCommitStore LoadVersionForOverwriting version 1 ")

	return commitStore, nil
}

func PrepareCommitStoreWithDB(storeKeys []string, iavl2Dir string, loadVersion int64, scRawDb corestore.KVStoreWithBatch) (*commitment.CommitStore, error) {
	metadata := commitment.NewMetadataStore(scRawDb)

	removedStoreKeys, err := metadata.GetRemovedStoreKeys(uint64(loadVersion))
	if err != nil {
		return nil, err
	}

	newTreeFn := func(key string) (commitment.CompatV1Tree, error) {
		dir := fmt.Sprintf("%s/%s", iavl2Dir, key) // .../data/iavl2/storekey
		fmt.Println("PrepareCommitStore newTreeFn dir: ", dir)
		dbOpts := sqlite.Options{Path: dir, Metrics: metrics.NoOpMetrics{}, Logger: _log.NewNopLogger()} // , OptimizeOnStart: true}

		return iavlv2.NewTree(itree3.DefaultOptions(), dbOpts, _log.NewNopLogger())
	}

	trees := make(map[string]commitment.CompatV1Tree, len(storeKeys))
	for _, key := range storeKeys {
		tree, err := newTreeFn(key)
		if err != nil {
			panic(err)
		}

		trees[key] = tree
	}

	oldTrees := make(map[string]commitment.CompatV1Tree, len(storeKeys))
	for _, key := range removedStoreKeys {
		tree, err := newTreeFn(string(key))
		if err != nil {
			return nil, err
		}
		oldTrees[string(key)] = tree
	}

	commitStore, err := commitment.NewCommitStore(trees, oldTrees, scRawDb, _log.NewNopLogger(), metrics.NoOpMetrics{})
	if err != nil {
		return nil, err
	}

	if err = commitStore.LoadVersion(uint64(loadVersion)); err != nil {
		fmt.Println("PrepareCommitStore LoadVersion failed, version: ", loadVersion, "err: ", err.Error())

		return nil, err
	}
	fmt.Println("PrepareCommitStore LoadVersion version: ", loadVersion)

	return commitStore, nil
}

func ApplyChangeSet(v1appPath string, storeKeys []string, from, to int64, v3CommitStore *commitment.CommitStore, v1appDB dbm.DB, dbType string) error {
	// get v1 app db
	var (
		appdb dbm.DB
		err   error
	)

	if v1appDB != nil {
		appdb = v1appDB
	} else {
		appdb, err = OpenDB(v1appPath, dbType)
		if err != nil {
			fmt.Println("panic position: OpenDB")
			return err
		}
		defer appdb.Close()
	}

	versionChangeset := &corestore.Changeset{}
	for v := from + 1; v <= to; v++ {
		fmt.Println("applying changeset version: ", v)

		versionChangeset.Version = uint64(v)
		versionChangeset.Changes = make([]corestore.StateChanges, 0)

		// new v1 ImmutableTree
		// var singleVersionChangeset []*iavl.ChangeSet
		for _, sk := range storeKeys {
			prefix := fmt.Sprintf("s/k:%s/", sk)
			fmt.Println("read tree dir: ", v1appPath, "version: ", v-1, "prefix: ", prefix)
			db := dbm.NewPrefixDB(appdb, []byte(prefix))

			mtree := iavl.NewMutableTree(wrapper.NewDBWrapper(db), 5000, false, iavl.NewNopLogger())
			mtreeLatestVersion, err := mtree.LoadVersion(v - 1)
			if err != nil {
				if errors.Is(err, iavl.ErrVersionDoesNotExist) {
					continue
				}
				return err
			}

			imtree := mtree.ImmutableTree
			fmt.Println("ApplyChangeset mtree latest version: ", mtreeLatestVersion, "imtree version: ", imtree.Version())

			imtree.TraverseStateChanges(v, v, func(ver int64, cs *iavl.ChangeSet) error {
				versionChangeset.Changes = append(versionChangeset.Changes, ConvertStateChanges(cs, sk, uint64(ver)))

				return nil
			})
		}
		fmt.Printf("versioned changesests: %+v, version: %d\n", versionChangeset, v)

		// apply changeset...
		if err := v3CommitStore.WriteChangeset(versionChangeset); err != nil {
			fmt.Println("ApplyChangeset WriteChangeset error")
			return err
		}

		_, err := v3CommitStore.Commit(uint64(v))
		if err != nil {
			fmt.Println("ApplyChangeset Commit error, v = ", v)

			return err
		}
	}
	return nil
}

func ApplyChangeSetAndCommitInfo(v1appPath string, storeKeys []string, from, to int64, v3CommitStore *commitment.CommitStore, dbType string) error {
	// get v1 app db
	appdb, err := OpenDB(v1appPath, dbType)
	if err != nil {
		fmt.Println("panic position: OpenDB")
		return err
	}
	defer appdb.Close()

	versionChangeset := &corestore.Changeset{}
	for v := from + 1; v <= to; v++ {
		if err := v3CommitStore.LoadVersionForOverwriting(uint64(v - 1)); err != nil {
			fmt.Println("ApplyChangeSet LoadVersionForOverwriting version", v-1, "error: ", err.Error())
			return err
		}

		versionChangeset.Version = uint64(v)
		versionChangeset.Changes = make([]corestore.StateChanges, 0)

		// new v1 ImmutableTree
		for _, sk := range storeKeys {
			prefix := fmt.Sprintf("s/k:%s/", sk)
			fmt.Println("read tree dir: ", v1appPath, "version: ", 1, "prefix: ", prefix)
			db := dbm.NewPrefixDB(appdb, []byte(prefix))

			mtree := iavl.NewMutableTree(wrapper.NewDBWrapper(db), 5000, false, iavl.NewNopLogger())
			mtreeLatestVersion, err := mtree.LoadVersion(v - 1)
			if err != nil {
				if errors.Is(err, iavl.ErrVersionDoesNotExist) {
					continue
				}
				return err
			}

			imtree := mtree.ImmutableTree
			fmt.Println("ApplyChangeset mtree latest version: ", mtreeLatestVersion, "imtree version: ", imtree.Version())

			imtree.TraverseStateChanges(v, v, func(ver int64, cs *iavl.ChangeSet) error {
				versionChangeset.Changes = append(versionChangeset.Changes, ConvertStateChanges(cs, sk, uint64(ver)))

				return nil
			})
		}
		fmt.Printf("versioned changesests: %+v, version: %d\n", versionChangeset, v)

		// apply changeset...
		if err = v3CommitStore.WriteChangeset(versionChangeset); err != nil {
			fmt.Println("ApplyChangeset WriteChangeset error")
			return err
		}

		commitInfo, err := v3CommitStore.Commit(uint64(v))
		if err != nil {
			fmt.Println("ApplyChangeset Commit error, v = ", v)

			return err
		}

		if err = v3CommitStore.FlushCommitInfo(commitInfo); err != nil {
			fmt.Println("ApplyChangeset FlushCommitInfo error")

			return err
		}
	}
	return nil
}

func ConvertStateChanges(cs *iavl.ChangeSet, sk string, version uint64) corestore.StateChanges {
	// return v3Changeset
	sc := corestore.StateChanges{}
	sc.Actor = []byte(sk)
	sc.StateChanges = make([]corestore.KVPair, len(cs.Pairs))
	for i, p := range cs.Pairs {
		sc.StateChanges[i] = corestore.KVPair{
			Key:    p.Key,
			Value:  p.Value,
			Remove: p.Delete,
		}
	}
	return sc
}

func applyChangeSetCommand() *cobra.Command { // migrate all version changeset from earliest+1 to latest height
	// ./sahara-store-migrate v1 apply-changesets --v1-app /Users/wenqi/.saharad/data/application.db --v2-app /Users/wenqi/.saharad/data/application-v2.db --v2-iavl2 /Users/wenqi/.saharad/data/iavl2 --root /Users/wenqi/.saharad --migrate-from 3275518

	var (
		// dataPath    string
		v2iavl2Path string
		v1appPath   string
		v2appPath   string
		rootPath    string
		concurrency int
		dbType      string
		migrateFrom int64
	)
	cmd := &cobra.Command{
		Use:   "apply-changesets",
		Short: "migrate latest iavl v0 application.db state to iavl v2 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			earliestHeight, latestHeight, err := fetchBlockRange(dbType, filepath.Join(rootPath, "data")) // latestHeight
			if err != nil {
				panic(err)
			}
			fmt.Printf("block height range: %d - %d\n", earliestHeight, latestHeight)

			// prepare dbs
			fmt.Println("Preparing DBs")
			v1db, err := core.NewStoreWithAppPath(v1appPath, core.Iavl1Type, dbType)
			if err != nil {
				return err
			}
			defer v1db.Close()

			v2db, err := core.NewDBWithAppPath(v2appPath, dbType)
			if err != nil {
				return err
			}
			defer v2db.Close()

			// Migrate metadata latestVersion
			// 0 means get latestVersion from v1 application.db
			var from, to int64
			bz, err := v1db.Get([]byte(latestVersionKey_v1))
			if err != nil {
				return err
			}

			i64 := &types.Int64Value{}
			err = proto.Unmarshal(bz, i64)
			if err != nil {
				return err
			}

			appLatestVersion := i64.Value
			fmt.Println("latest version from v0 application is ", appLatestVersion)

			if appLatestVersion < earliestHeight {
				return errors.New("")
			}
			if appLatestVersion < latestHeight {
				from, to = earliestHeight, appLatestVersion
			} else {
				from, to = earliestHeight, latestHeight
			}

			// Migrate historic changesets
			// commitStore.metadata base db path is root/data/application.db
			var storeKeys []string
			for k := range v1db.CommitInfoByName() {
				storeKeys = append(storeKeys, k)
			}

			fmt.Println("Start applying changesets")
			if migrateFrom > 0 {
				from = migrateFrom
			}

			// 使用实际的起始版本加载 commit store
			startVersion := earliestHeight
			if migrateFrom > 0 && migrateFrom > earliestHeight {
				startVersion = migrateFrom // 从 migrateFrom 开始，因为 ApplyChangeSet 会从 from+1 开始处理
			}

			fmt.Println("Preparing CommitStore with start version:", startVersion)
			commitStore, err := PrepareCommitStoreWithDB(storeKeys, v2iavl2Path, startVersion, v2db)
			if err != nil {
				panic(err)
			}
			if err = ApplyChangeSet(v1appPath, storeKeys, from, to, commitStore, v1db.DB, dbType); err != nil {
				panic(err)
			}

			return nil
		},
	}
	// cmd.Flags().StringVar(&v1dataPath, "v1-data", "", "Path to the v1 data/")
	cmd.Flags().StringVar(&v2iavl2Path, "v2-iavl2", "", "Path to the iavl2/")
	cmd.Flags().StringVar(&v1appPath, "v1-app", "", "Path to the v1 application db")
	cmd.Flags().StringVar(&v2appPath, "v2-app", "", "Path to the v2 application db")
	cmd.Flags().StringVar(&rootPath, "root", "", "Path to the .saharad")
	cmd.Flags().StringVar(&dbType, "db-type", goleveldbType, "db type of application.db")

	if err := cmd.MarkFlagRequired("v1-app"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("v2-app"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("v2-iavl2"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("root"); err != nil {
		panic(err)
	}
	cmd.Flags().IntVar(&concurrency, "concurrency", 6, "Number of concurrent migrations")
	cmd.Flags().Int64Var(&migrateFrom, "migrate-from", 0, "migrate from version, default 0 means the earliest version")

	return cmd
}
