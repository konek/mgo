package mgo

import (
	"container/list"

	"gopkg.in/mgo.v2"
)

// DbQueueFn ...
type DbQueueFn func(*Database, chan error)

// DbQueueFn2 ...
type DbQueueFn2 func(*Database)

// Database ...
type Database struct {
	*mgo.Database
}

// ENotFound ...
type ENotFound struct{}

// Error ...
func (e ENotFound) Error() string {
	return "not found"
}

// DbQueue ...
type DbQueue struct {
	queue chan DbQueueFn2
	dbs   *list.List
}

// NewDbQueue ...
func NewDbQueue(size int) *DbQueue {
	return &DbQueue{
		queue: make(chan DbQueueFn2, size),
		dbs:   list.New(),
	}
}

// Push ...
func (q *DbQueue) Push(cb DbQueueFn) error {
	var err error

	errc := make(chan error)
	q.queue <- func(db *Database) {
		cb(db, errc)
	}
	err = <-errc
	return err
}

// Count ...
func (q *DbQueue) Count(collection string, query interface{}) (int, error) {
	var n int
	err := q.Push(func(db *Database, ec chan error) {
		var e error
		q := db.C(collection).Find(query)
		n, e = q.Count()
		ec <- e
	})
	return n, err
}

// FindOne ...
func (q *DbQueue) FindOne(collection string, ret interface{}, query interface{}) error {
	err := q.Push(func(db *Database, ec chan error) {
		query := db.C(collection).Find(query)
		n, e := query.Count()
		if e != nil {
			ec <- e
			return
		}
		if n == 0 {
			ec <- ENotFound{}
			return
		}
		ec <- query.One(ret)
	})
	return err
}

// Find ...
func (q *DbQueue) Find(collection string, ret interface{}, query interface{}) error {
	err := q.Push(func(db *Database, ec chan error) {
		query := db.C(collection).Find(query)
		n, e := query.Count()
		if e != nil {
			ec <- e
			return
		}
		if n == 0 {
			ec <- ENotFound{}
			return
		}
		ec <- query.All(ret)
	})
	return err
}

// RunDb ...
func (q *DbQueue) RunDb(db *Database) {
	for {
		fn := <-q.queue
		fn(db)
	}
}

// Run ...
func (q *DbQueue) Run() {
	for e := q.dbs.Front(); e != nil; e = e.Next() {
		db, ok := e.Value.(*Database)
		if ok == false {
			continue
		}
		go q.RunDb(db)
	}
}

// AddConnection ...
func (q *DbQueue) AddConnection(url string, name string) error {
	session, err := mgo.Dial(url)
	if err != nil {
		return err
	}
	db := session.DB(name)
	q.dbs.PushBack(&Database{db})
	return nil
}

// Ref ...
func (q *DbQueue) Ref(collection string, id interface{}) mgo.DBRef {
	return mgo.DBRef{
		Collection: collection,
		Id:         id,
	}
}
