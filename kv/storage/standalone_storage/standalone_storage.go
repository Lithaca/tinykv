package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db *badger.DB
}

type StandAloneReader struct{
	txn *badger.Txn
}

func (s StandAloneReader) GetCF(cf string, key []byte) ([]byte,error) {
	item,err := s.txn.Get(engine_util.KeyWithCF(cf,key))
	return item.Key(),err
}

func (s StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf,s.txn)
}

func (s StandAloneReader) Close() {
	s.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{conf,nil}
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.ValueDir = s.conf.DBPath
	opts.Dir = s.conf.DBPath
	db,err:=badger.Open(opts)
	if err != nil {
		panic(err)
	}
	s.db=db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return StandAloneReader{txn},nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _,m:=range batch{
		switch m.Data.(type){
		case storage.Put:
			put:=m.Data.(storage.Put)
			err := s.db.Update(func(txn *badger.Txn) error{
				err :=txn.Set(engine_util.KeyWithCF(put.Cf,put.Key),put.Value)
				return err
			})
			return err
		case storage.Delete:
			put:=m.Data.(storage.Put)
			err := s.db.Update(func(txn *badger.Txn) error{
				err :=txn.Set(engine_util.KeyWithCF(put.Cf,put.Key),[]byte(""))
				return err
			})
			return err
		}
	}
	return nil
}
