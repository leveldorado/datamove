package move

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Mover struct {
	source *mgo.Session
	target *mgo.Session
}

func NewMover(source, target *mgo.Session) *Mover {
	return &Mover{source: source, target: target}
}

func (m *Mover) MoveCollection(database, collection string, errChan chan error) {
	//skip admin system.version
	if database == "admin" && collection == "system.version" {
		errChan <- nil
		return
	}

	sourceCol := m.source.Copy().DB(database).C(collection)
	defer sourceCol.Database.Session.Close()
	targetCol := m.target.Copy().DB(database).C(collection)
	defer targetCol.Database.Session.Close()

	counter := 0

	iter := sourceCol.Find(bson.M{}).Iter()
	doc := bson.M{}
	for iter.Next(&doc) {
		if err := targetCol.Insert(doc); err != nil {
			iter.Close()
			errChan <- err
			return
		}
		doc = bson.M{}
		counter++
	}
	if err := iter.Close(); err != nil {
		errChan <- err
		return
	}
	errChan <- nil
	fmt.Println("COPIED docs:", counter, "database:", database, "collection:", collection)
}

func (m *Mover) MoveDatabase(database string, errChan chan error, endChan chan struct{}) {
	db := m.source.DB(database)
	collections, err := db.CollectionNames()
	if err != nil {
		errChan <- err
		return
	}
	collectionMoveErrChan := make(chan error, len(collections))
	for _, col := range collections {
		go m.MoveCollection(database, col, collectionMoveErrChan)
	}

	for i := 0; i < len(collections); i++ {
		if err := <-collectionMoveErrChan; err != nil {
			errChan <- err
		}
	}
	close(collectionMoveErrChan)
	fmt.Println("MOVED database", database)
	endChan <- struct{}{}
}

func (m *Mover) MoveDatabases(databases []string) (errorOutput chan error) {
	ch := make(chan error, len(databases))
	endChan := make(chan struct{}, len(databases))
	go func() {
		for _, v := range databases {
			go m.MoveDatabase(v, ch, endChan)
		}
	}()
	go func() {
		for i := 0; i < len(databases); i++ {
			_ = <-endChan
		}
		close(ch)
	}()
	return ch
}

func (m *Mover) MoveAllDatabases() chan error {
	databases, err := m.source.DatabaseNames()
	if err != nil {
		ch := make(chan error, 1)
		ch <- err
		close(ch)
		return ch
	}
	return m.MoveDatabases(databases)
}
