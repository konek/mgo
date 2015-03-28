package mgo

import (
	"container/list"

	"gopkg.in/mgo.v2"
)

type DbQueueFn func (*Database, chan error)
type DbQueueFn2 func (*Database)

type Database struct{
	*mgo.Database
}

type DbQueue struct{
	queue chan DbQueueFn2
	dbs *list.List
}

func NewDbQueue(size int) *DbQueue {
	return &DbQueue{
		queue: make(chan DbQueueFn2, size),
		dbs: list.New(),
	}
}

func (q *DbQueue) Push(cb DbQueueFn) error {
	var err error

	errc := make(chan error)
	q.queue <- func (db *Database) {
		cb(db, errc)
	}
	err = <-errc
	return err
}

func (q *DbQueue) RunDb(db *Database) {
	for {
		fn := <-q.queue
		fn(db)
	}
}

func (q *DbQueue) Run() {
	for e := q.dbs.Front(); e != nil; e = e.Next() {
		db, ok := e.Value.(*Database)
		if ok == false {
			continue
		}
		go q.RunDb(db)
	}
}

func (q *DbQueue) AddConnection(url string, name string) error {
	session, err := mgo.Dial(url)
	if err != nil {
		return err
	}
	db := session.DB(name)
	q.dbs.PushBack(&Database{db})
	return nil
}

