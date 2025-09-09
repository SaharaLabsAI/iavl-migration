package core

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	store "cosmossdk.io/api/cosmos/store/v1beta1"
	"github.com/SaharaLabsAI/sahara-store/sdk/proof"
	db "github.com/cosmos/cosmos-db"
	"github.com/cosmos/gogoproto/proto"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/kocubinski/costor-api/logz"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	// "github.com/cosmos/iavl"
)

const (
	CommitInfoKeyFmt = "s/%d" // s/<version>
	LatestVersionKey = "s/latest"

	CommitInfoKeyFmt_v3 = "c/%d" // s/<version>
	LatestVersionKey_v3 = "c/latest"

	goleveldbType = "goleveldb"
	pebbledbType  = "pebbledb"
)

var (
	log       = logz.Logger.With().Str("module", "store").Logger()
	dbOpenMtx sync.Mutex
)

const (
	Iavl1Type int = iota
	Iavl2Type
)

type ReadonlyStore struct {
	db.DB
	commitInfoByName map[string]*store.CommitInfo
}

func (rs *ReadonlyStore) CommitInfoByName() map[string]*store.CommitInfo {
	return rs.commitInfoByName
}

func (rs *ReadonlyStore) getCommitInfoFromDB(ver int64, ciKey string) (*store.CommitInfo, error) {
	cInfoKey := fmt.Sprintf(ciKey, ver)

	bz, err := rs.DB.Get([]byte(cInfoKey))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get commit info")
	} else if bz == nil {
		return nil, errors.New("no commit info found")
	}
	// fmt.Printf("commitInfo bz: %x\n", bz)
	cInfo := &store.CommitInfo{}
	if err = proto.Unmarshal(bz, cInfo); err != nil {
		return nil, errors.Wrap(err, "failed unmarshal commit info")
	}

	return cInfo, nil
}

func (rs *ReadonlyStore) getCommitInfoFromDBV3(ver int64, ciKey string) (*proof.CommitInfo, error) { // (*store.CommitInfo, error) {
	cInfoKey := fmt.Sprintf(ciKey, ver)

	bz, err := rs.DB.Get([]byte(cInfoKey))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get commit info")
	} else if bz == nil {
		return nil, errors.New("no commit info found")
	}
	// fmt.Printf("v3 commitInfo bz: %x\n", bz)
	// cInfo := &store.CommitInfo{}
	// if err = proto.Unmarshal(bz, cInfo); err != nil {
	// 	return nil, errors.Wrap(err, "failed unmarshal commit info")
	// }
	cInfo := &proof.CommitInfo{}
	if err = cInfo.Unmarshal(bz); err != nil {
		return nil, errors.Wrap(err, "failed unmarshal commit info")
	}

	return cInfo, nil
}

func NewReadonlyStore(dbPath string, dbType string) (*ReadonlyStore, error) {
	l := log.With().
		Str("dbPath", fmt.Sprintf("%s/application.db", dbPath)).
		Str("op", "NewReadonlyStore").
		Logger()
	since := time.Now()
	l.Info().Msg("waiting for lock")
	dbOpenMtx.Lock()
	l.Info().Msgf("got lock in %s", time.Since(since))
	defer dbOpenMtx.Unlock()

	var err error
	rs := &ReadonlyStore{
		commitInfoByName: make(map[string]*store.CommitInfo),
	}
	since = time.Now()

	switch dbType {
	case goleveldbType:
		rs.DB, err = db.NewGoLevelDBWithOpts("application", dbPath, &opt.Options{
			ReadOnly: true,
		})
	case pebbledbType:
		rs.DB, err = db.NewPebbleDB("application", dbPath, nil)
	}
	if err != nil {
		return nil, err
	}

	l.Info().Msgf("opened in %s", time.Since(since))

	// fmt.Printf("rs.db.Stats = %+v \n", rs.Stats())
	// it, err := rs.Iterator(nil, nil)
	// if err != nil {
	// 	fmt.Println("iterator err")
	// 	panic(err)
	// }
	// defer it.Close()

	// for ; it.Valid(); it.Next() {
	// 	key := it.Key()
	// 	value := it.Value()
	// 	fmt.Printf("key: %x, value: %x\n", key, value)
	// }

	latestVersionBz, err := rs.DB.Get([]byte(LatestVersionKey))
	if err != nil {
		return nil, err
	}

	var latestVersion int64
	if err := gogotypes.StdInt64Unmarshal(&latestVersion, latestVersionBz); err != nil {
		return nil, err
	}
	fmt.Println("core/store latestVersion: ", latestVersion)
	since = time.Now()
	commitInfo, err := rs.getCommitInfoFromDB(latestVersion, CommitInfoKeyFmt)
	if err != nil {
		return nil, err
	}

	var storeInfoNames []string
	for _, si := range commitInfo.StoreInfos {
		storeInfoNames = append(storeInfoNames, si.Name)
		rs.commitInfoByName[si.Name] = commitInfo
	}
	log.Info().Msgf("loaded commit info in %s for stores %s", time.Since(since),
		strings.Join(storeInfoNames, " "))

	return rs, nil
}

func NewReadonlyStoreForV3(dbPath string, iavlVersion string, dbType string) (*ReadonlyStore, error) {
	var latestVersionKey, commitInfoKeyFmt string

	latestVersionKey, commitInfoKeyFmt = "c/latest", "c/%d"

	l := log.With().
		Str("dbPath", fmt.Sprintf("%s/application.db", dbPath)).
		Str("op", "NewReadonlyStore").
		Logger()
	since := time.Now()
	l.Info().Msg("waiting for lock")
	dbOpenMtx.Lock()
	l.Info().Msgf("got lock in %s", time.Since(since))
	defer dbOpenMtx.Unlock()

	var err error
	rs := &ReadonlyStore{
		commitInfoByName: make(map[string]*store.CommitInfo),
	}
	since = time.Now()
	switch dbType {
	case goleveldbType:
		rs.DB, err = db.NewGoLevelDBWithOpts("application", dbPath, &opt.Options{
			ReadOnly: true,
		})
	case pebbledbType:
		rs.DB, err = db.NewPebbleDB("application", dbPath, nil)
	}
	if err != nil {
		return nil, err
	}

	l.Info().Msgf("opened in %s", time.Since(since))

	latestVersionBz, err := rs.DB.Get([]byte(latestVersionKey))
	if err != nil {
		return nil, err
	}
	fmt.Printf("latestVersionBz: %x\n", latestVersionBz)

	var latestVersion int64
	// ok
	// var latestVersion int64 = int64(latestVersionBz[0])

	// ok
	switch len(latestVersionBz) {
	case 1:
		latestVersion = int64(latestVersionBz[0])
	case 8:
		latestVersion = int64(binary.BigEndian.Uint64(latestVersionBz))
	default:
		panic(fmt.Sprintf("unexpected length: %d", len(latestVersionBz)))
	}
	fmt.Println(latestVersion) // 5

	// only v1 ok
	// if err := gogotypes.StdInt64Unmarshal(&latestVersion, latestVersionBz); err != nil {
	// 	return nil, err
	// }
	fmt.Println("core/store latestVersion: ", latestVersion)
	since = time.Now()
	commitInfo, err := rs.getCommitInfoFromDBV3(latestVersion, commitInfoKeyFmt)
	if err != nil {
		return nil, err
	}

	var storeInfoNames []string
	for _, si := range commitInfo.StoreInfos {
		storeInfoNames = append(storeInfoNames, string(si.Name))
		// rs.commitInfoByName[string(si.Name)] = commitInfo
	}
	log.Info().Msgf("loaded commit info in %s for stores %s", time.Since(since),
		strings.Join(storeInfoNames, " "))

	return rs, nil
}

func NewStoreWithAppPath(appPath string, iavlType int, dbType string) (*ReadonlyStore, error) {
	var (
		latestVersionKey string
		commitInfoKeyFmt string
	)
	switch iavlType {
	case Iavl1Type:
		latestVersionKey, commitInfoKeyFmt = LatestVersionKey, CommitInfoKeyFmt
	case Iavl2Type:
		latestVersionKey, commitInfoKeyFmt = LatestVersionKey_v3, CommitInfoKeyFmt_v3
	default:
		return nil, errors.New("wrong iavl type")
	}
	var err error
	rs := &ReadonlyStore{
		commitInfoByName: make(map[string]*store.CommitInfo),
	}

	applicationDir := filepath.Dir(appPath)
	applicationFileName := filepath.Base(appPath)
	applicationFileNameWithoutExt := strings.TrimSuffix(applicationFileName, filepath.Ext(".db"))
	fmt.Printf("get from apppath: applicationFileNameWithoutExt: %s, applicationDir: %s\n", applicationFileNameWithoutExt, applicationDir)

	switch dbType {
	case goleveldbType:
		rs.DB, err = db.NewGoLevelDBWithOpts(applicationFileNameWithoutExt, applicationDir, &opt.Options{
			ReadOnly: false,
		})
	case pebbledbType:
		rs.DB, err = db.NewPebbleDB(applicationFileNameWithoutExt, applicationDir, nil)
	}
	if err != nil {
		return nil, err
	}

	latestVersionBz, err := rs.DB.Get([]byte(latestVersionKey))
	if err != nil {
		return nil, err
	}

	var latestVersion int64
	if err := gogotypes.StdInt64Unmarshal(&latestVersion, latestVersionBz); err != nil {
		return nil, err
	}
	fmt.Println("get latestVersion from application db", "appPath", appPath, "latestVersion", latestVersion)

	commitInfo, err := rs.getCommitInfoFromDB(latestVersion, commitInfoKeyFmt)
	if err != nil {
		return nil, err
	}

	var storeInfoNames []string
	for _, si := range commitInfo.StoreInfos {
		storeInfoNames = append(storeInfoNames, si.Name)
		rs.commitInfoByName[si.Name] = commitInfo
	}

	return rs, nil
}

func NewStore(dbPath string, dbType string) (*ReadonlyStore, error) {
	fmt.Println("NewStore dbPath: ", dbPath)

	l := log.With().
		Str("dbPath", fmt.Sprintf("%s/application.db", dbPath)).
		Str("op", "NewStore").
		Logger()
	since := time.Now()
	l.Info().Msg("waiting for lock")
	dbOpenMtx.Lock()
	l.Info().Msgf("got lock in %s", time.Since(since))
	defer dbOpenMtx.Unlock()

	var err error
	rs := &ReadonlyStore{
		commitInfoByName: make(map[string]*store.CommitInfo),
	}
	fmt.Println("NewStore dbPath: ", dbPath)

	switch dbType {
	case goleveldbType:
		rs.DB, err = db.NewGoLevelDBWithOpts("application", dbPath, &opt.Options{
			ReadOnly: false,
		})
	case pebbledbType:
		rs.DB, err = db.NewPebbleDB("application", dbPath, nil)
	}

	if err != nil {
		panic(err)
	}

	return rs, nil
}

func NewDBWithAppPath(appPath string, dbType string) (*ReadonlyStore, error) {
	var err error
	rs := &ReadonlyStore{
		commitInfoByName: make(map[string]*store.CommitInfo),
	}

	applicationDir := filepath.Dir(appPath)
	applicationFileName := filepath.Base(appPath)
	applicationFileNameWithoutExt := strings.TrimSuffix(applicationFileName, filepath.Ext(".db"))
	fmt.Printf("get from apppath: applicationFileNameWithoutExt: %s, applicationDir: %s", applicationFileNameWithoutExt, applicationDir)
	switch dbType {
	case goleveldbType:
		rs.DB, err = db.NewGoLevelDBWithOpts(applicationFileNameWithoutExt, applicationDir, &opt.Options{})
	case pebbledbType:
		rs.DB, err = db.NewPebbleDB(applicationFileNameWithoutExt, applicationDir, nil)
	}
	if err != nil {
		return nil, err
	}
	return rs, nil
}

var ErrStoreNotFound = errors.New("store not found")
