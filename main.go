package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/leveldorado/datamove/move"
	"gopkg.in/mgo.v2"
)

var sourceHost string
var targetHost string
var database string
var collection string

const (
	allDatabases = "all"
)

func main() {
	parseFlags()
	mover := move.NewMover(MustGetMgoSession(sourceHost), MustGetMgoSession(targetHost))
	collections := make(map[string]bool)
	if collection != "" {
		for _, k := range strings.Split(collection, ",") {
			collections[k] = true
		}
	}
	var errChann chan error
	if database == allDatabases {
		errChann = mover.MoveAllDatabases(collections)
	} else {
		databases := strings.Split(database, ",")
		errChann = mover.MoveDatabases(databases, collections)
	}
	for err := range errChann {
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("Job completed.")
}

func MustGetMgoSession(host string) *mgo.Session {
	s, err := mgo.Dial(host)
	if err != nil {
		panic(err)
	}
	return s
}

func parseFlags() {
	flag.StringVar(&sourceHost, "source", "", "--source=localhost:27017")
	flag.StringVar(&targetHost, "target", "", "--target=localhost:27017")
	flag.StringVar(&database, "database", "all", "--database=local,test")
	flag.StringVar(&collection, "collection", "", "--collection=person,product")
	flag.Parse()
	if sourceHost == "" {
		fmt.Println("flag --source is required, value must be host and port of source mongodb instance")
		os.Exit(1)
	}
	if targetHost == "" {
		fmt.Println("flag --target is required, value must be host and port of target mongodb instance")
		os.Exit(1)
	}
}
