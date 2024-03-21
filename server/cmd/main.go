package main

import (
	"flag"
	"fmt"
	"github.com/go-raft/db"
	"github.com/go-raft/logging"
	"github.com/go-raft/model"
	"github.com/go-raft/server"
	"log"
	"math/rand"
	"net"
	"time"
)

var (
	serverName = flag.String("server-name", "", "name for the server")
	port       = flag.String("port", "", "port for running the server")
)

const (
	ElectionMinTimeout = 3001
	ElectionMaxTimeout = 10000
)

func main() {
	parseFlags()
	l, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	database, err := db.NewDatabase()
	if err != nil {
		fmt.Println("Error while creating database")
		return
	}

	rand.Seed(time.Now().UnixNano())
	electionTimeoutInterval := rand.Intn(ElectionMaxTimeout-ElectionMinTimeout) + int(ElectionMinTimeout)
	electionModule := model.NewElectionModule(electionTimeoutInterval)

	err = logging.RegisterServer(*serverName, *port)
	if err != nil {
		fmt.Println(err)
		return
	}

	s := server.Server{
		Port:           *port,
		Db:             database,
		Logs:           database.RebuildLogIfExists(*serverName),
		ServerState:    model.GetExistingServerStateOrCreateNew(*serverName),
		CurrentRole:    "follower",
		LeaderNodeId:   "",
		Peerdata:       model.NewPeerData(),
		ElectionModule: electionModule,
	}
	s.ServerState.LogServerPersistedState()
	go s.ElectionTimer()
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go s.HandleConnection(c)
	}
}

func parseFlags() {
	flag.Parse()

	if *serverName == "" {
		log.Fatalf("Must provide serverName for the server")
	}

	if *port == "" {
		log.Fatalf("Must provide a port number for server to run")
	}
}
