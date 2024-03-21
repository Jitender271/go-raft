package server

import (
	"bufio"
	"fmt"
	"github.com/go-raft/db"
	"github.com/go-raft/logging"
	"github.com/go-raft/model"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	BroadcastPeriod = 3000
)

type Server struct {
	Port           string
	Db             *db.Database
	ServerState    *model.ServerState
	Logs           []string
	CurrentRole    string
	LeaderNodeId   string
	Peerdata       *model.PeerData
	ElectionModule *model.ElectionModule
}

func (s *Server) sendMessageToFollowerNode(message string, port int) {
	c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		s.Peerdata.SuspectedNodes[port] = true
		return
	}
	_, ok := s.Peerdata.SuspectedNodes[port]
	if ok {
		delete(s.Peerdata.SuspectedNodes, port)
	}
	fmt.Fprintf(c, message+"\n")
	go s.HandleConnection(c)
}

func (s *Server) replicateLog(followerName string, followerPort int) {
	if followerName == s.ServerState.Name {
		go s.commitLogEntries()
		return
	}
	var prefixTerm = 0
	prefixLength := s.Peerdata.SentLength[followerName]
	if prefixLength > 0 {
		logSplit := strings.Split(s.Logs[prefixLength-1], "#")
		prefixTerm, _ = strconv.Atoi(logSplit[1])
	}
	logRequest := model.NewLogRequest(s.ServerState.Name, s.ServerState.CurrentTerm, prefixLength, prefixTerm, s.ServerState.CommitLength, s.Logs[s.Peerdata.SentLength[followerName]:])
	s.sendMessageToFollowerNode(logRequest.String(), followerPort)
}

func (s *Server) addLogs(log string) []string {
	s.Logs = append(s.Logs, log)
	return s.Logs
}

func (s *Server) appendEntries(prefixLength int, commitLength int, suffix []string) {
	if len(suffix) > 0 && len(s.Logs) > prefixLength {
		var index int
		if len(s.Logs) > (prefixLength + len(suffix)) {
			index = prefixLength + len(suffix) - 1
		} else {
			index = len(s.Logs) - 1
		}
		if parseLogTerm(s.Logs[index]) != parseLogTerm(suffix[index-prefixLength]) {
			s.Logs = s.Logs[:prefixLength]
		}
	}
	if prefixLength+len(suffix) > len(s.Logs) {
		for i := len(s.Logs) - prefixLength; i < len(suffix); i++ {
			s.addLogs(suffix[i])
			err := s.Db.LogCommand(suffix[i], s.ServerState.Name)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	if commitLength > s.ServerState.CommitLength {
		for i := s.ServerState.CommitLength; i < commitLength; i++ {
			s.Db.PerformDbOperations(strings.Split(s.Logs[i], "#")[0])
		}
		s.ServerState.CommitLength = commitLength
		s.ServerState.LogServerPersistedState()
	}
}

func (s *Server) handleLogResponse(message string) string {
	lr, _ := model.ParseLogResponse(message)
	if lr.CurrentTerm > s.ServerState.CurrentTerm {
		s.ServerState.CurrentTerm = lr.CurrentTerm
		s.CurrentRole = "follower"
		s.ServerState.VotedFor = ""
		go s.ElectionTimer()
	}
	if lr.CurrentTerm == s.ServerState.CurrentTerm && s.CurrentRole == "leader" {
		if lr.ReplicationSuccessful && lr.AckLength >= s.Peerdata.AckedLength[lr.NodeId] {
			s.Peerdata.SentLength[lr.NodeId] = lr.AckLength
			s.Peerdata.AckedLength[lr.NodeId] = lr.AckLength
			s.commitLogEntries()
		} else {
			s.Peerdata.SentLength[lr.NodeId] = s.Peerdata.SentLength[lr.NodeId] - 1
			s.replicateLog(lr.NodeId, lr.Port)
		}
	}
	return "replication successful"
}

func (s *Server) handleLogRequest(message string) string {
	s.ElectionModule.ResetElectionTimer <- struct{}{}
	logRequest, _ := model.ParseLogRequest(message)
	if logRequest.CurrentTerm > s.ServerState.CurrentTerm {
		s.ServerState.CurrentTerm = logRequest.CurrentTerm
		s.ServerState.VotedFor = ""
	}
	if logRequest.CurrentTerm == s.ServerState.CurrentTerm {
		if s.CurrentRole == "leader" {
			go s.ElectionTimer()
		}
		s.CurrentRole = "follower"
		s.LeaderNodeId = logRequest.LeaderId
	}
	var logOk = false
	if len(s.Logs) >= logRequest.PrefixLength &&
		(logRequest.PrefixLength == 0 ||
			parseLogTerm(s.Logs[logRequest.PrefixLength-1]) == logRequest.PrefixTerm) {
		logOk = true
	}
	port, _ := strconv.Atoi(s.Port)
	if s.ServerState.CurrentTerm == logRequest.CurrentTerm && logOk {
		s.appendEntries(logRequest.PrefixLength, logRequest.CommitLength, logRequest.Suffix)
		ack := logRequest.PrefixLength + len(logRequest.Suffix)
		return model.NewLogResponse(s.ServerState.Name, port, s.ServerState.CurrentTerm, ack, true).String()
	} else {
		return model.NewLogResponse(s.ServerState.Name, port, s.ServerState.CurrentTerm, 0, false).String()
	}
}

func (s *Server) commitLogEntries() {
	allNodes, _ := logging.ListRegisteredServer()
	eligbleNodeCount := len(allNodes) - len(s.Peerdata.SuspectedNodes)
	for i := s.ServerState.CommitLength; i < len(s.Logs); i++ {
		var acks = 0
		for node := range allNodes {
			if s.Peerdata.AckedLength[node] > s.ServerState.CommitLength {
				acks = acks + 1
			}
		}
		if acks >= (eligbleNodeCount+1)/2 || eligbleNodeCount == 1 {
			log := s.Logs[i]
			command := strings.Split(log, "#")[0]
			s.Db.PerformDbOperations(command)
			s.ServerState.CommitLength = s.ServerState.CommitLength + 1
			s.ServerState.LogServerPersistedState()
		} else {
			break
		}
	}
}

func (s *Server) handleVoteRequest(message string) string {
	voteRequest, _ := model.ParseVoteRequest(message)
	if voteRequest.CandidateTerm > s.ServerState.CurrentTerm {
		s.ServerState.CurrentTerm = voteRequest.CandidateTerm
		s.CurrentRole = "follower"
		s.ServerState.VotedFor = ""
		s.ElectionModule.ResetElectionTimer <- struct{}{}
	}
	var lastTerm = 0
	if len(s.Logs) > 0 {
		lastTerm = parseLogTerm(s.Logs[len(s.Logs)-1])
	}
	var logOk = false
	if voteRequest.CandidateLogTerm > lastTerm ||
		(voteRequest.CandidateLogTerm == lastTerm && voteRequest.CandidateLogLength >= len(s.Logs)) {
		logOk = true
	}
	if voteRequest.CandidateTerm == s.ServerState.CurrentTerm && logOk && (s.ServerState.VotedFor == "" || s.ServerState.VotedFor == voteRequest.CandidateId) {
		s.ServerState.VotedFor = voteRequest.CandidateId
		s.ServerState.LogServerPersistedState()
		return model.NewVoteResponse(
			s.ServerState.Name,
			s.ServerState.CurrentTerm,
			true,
		).String()
	} else {
		return model.NewVoteResponse(s.ServerState.Name, s.ServerState.CurrentTerm, false).String()
	}
}

func (s *Server) handleVoteResponse(message string) {
	voteResponse, _ := model.ParseVoteResponse(message)
	if voteResponse.CurrentTerm > s.ServerState.CurrentTerm {
		if s.CurrentRole != "leader" {
			s.ElectionModule.ResetElectionTimer <- struct{}{}
		}
		s.ServerState.CurrentTerm = voteResponse.CurrentTerm
		s.CurrentRole = "follower"
		s.ServerState.VotedFor = ""
	}
	if s.CurrentRole == "candidate" && voteResponse.CurrentTerm == s.ServerState.CurrentTerm && voteResponse.VoteInFavor {
		s.Peerdata.VotesReceived[voteResponse.NodeId] = true
		s.checkForElectionResult()
	}
}

func (s *Server) HandleConnection(c net.Conn) {
	defer c.Close()
	for {
		data, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			continue
		}
		message := strings.TrimSpace(string(data))
		if message == "invalid command" || message == "replication successful" {
			continue
		}
		fmt.Println(">", string(message))
		var response = ""
		if strings.HasPrefix(message, "LogRequest") {
			response = s.handleLogRequest(message)
		}
		if strings.HasPrefix(message, "LogResponse") {
			response = s.handleLogResponse(message)
		}
		if strings.HasPrefix(message, "VoteRequest") {
			response = s.handleVoteRequest(message)
		}
		if strings.HasPrefix(message, "VoteResponse") {
			s.handleVoteResponse(message)
		}
		if s.CurrentRole == "leader" && response == "" {
			var err = s.Db.ValidateCommand(message)
			if err != nil {
				response = err.Error()
			}
			if strings.HasPrefix(message, "GET") {
				response = s.Db.PerformDbOperations(message)
			}
			if response == "" {
				logMessage := message + "#" + strconv.Itoa(s.ServerState.CurrentTerm)
				s.Peerdata.AckedLength[s.ServerState.Name] = len(s.Logs)
				s.Logs = append(s.Logs, logMessage)
				currLogIdx := len(s.Logs) - 1
				err = s.Db.LogCommand(logMessage, s.ServerState.Name)
				if err != nil {
					response = "error while logging command"
				}
				allServers, _ := logging.ListRegisteredServer()
				for sname, sport := range allServers {
					s.replicateLog(sname, sport)
				}
				for s.ServerState.CommitLength <= currLogIdx {
					fmt.Println("Waiting for consensus: ")
				}
				response = "operation sucessful"
			}
		}
		if response != "" {
			c.Write([]byte(response + "\n"))
		}
	}
}

func (s *Server) checkForElectionResult() {
	if s.CurrentRole == "leader" {
		return
	}
	var totalVotes = 0
	for server := range s.Peerdata.VotesReceived {
		if s.Peerdata.VotesReceived[server] {
			totalVotes += 1
		}
	}
	allNodes, _ := logging.ListRegisteredServer()
	if totalVotes >= (len(allNodes)+1)/2 {
		fmt.Println("I won the election. New leader: ", s.ServerState.Name, " Votes received: ", totalVotes)
		s.CurrentRole = "leader"
		s.LeaderNodeId = s.ServerState.Name
		s.Peerdata.VotesReceived = make(map[string]bool)
		s.ElectionModule.ElectionTimeout.Stop()
		s.syncUp()
	}
}

func (s *Server) startElection() {
	s.ServerState.CurrentTerm = s.ServerState.CurrentTerm + 1
	s.CurrentRole = "candidate"
	s.ServerState.VotedFor = s.ServerState.Name
	s.Peerdata.VotesReceived = map[string]bool{}
	s.Peerdata.VotesReceived[s.ServerState.Name] = true
	var lastTerm = 0
	if len(s.Logs) > 0 {
		lastTerm = parseLogTerm(s.Logs[len(s.Logs)-1])
	}
	voteRequest := model.NewVoteRequest(s.ServerState.Name, s.ServerState.CurrentTerm, len(s.Logs), lastTerm)
	allNodes, _ := logging.ListRegisteredServer()
	for node, port := range allNodes {
		if node != s.ServerState.Name {
			s.sendMessageToFollowerNode(voteRequest.String(), port)
		}
	}
	s.checkForElectionResult()
}

func (s *Server) ElectionTimer() {
	for {
		select {
		case <-s.ElectionModule.ElectionTimeout.C:
			fmt.Println("Timed out")
			if s.CurrentRole == "follower" {
				go s.startElection()
			} else {
				s.CurrentRole = "follower"
				s.ElectionModule.ResetElectionTimer <- struct{}{}
			}
		case <-s.ElectionModule.ResetElectionTimer:
			fmt.Println("Resetting election timer")
			s.ElectionModule.ElectionTimeout.Reset(time.Duration(s.ElectionModule.ElectionTimeoutInterval) * time.Millisecond)
		}
	}
}

func (s *Server) syncUp() {
	ticker := time.NewTicker(BroadcastPeriod * time.Millisecond)
	for t := range ticker.C {
		fmt.Println("sending heartbeat at: ", t)
		allServers, _ := logging.ListRegisteredServer()
		for sname, sport := range allServers {
			if sname != s.ServerState.Name {
				s.replicateLog(sname, sport)
			}
		}
	}
}

func parseLogTerm(message string) int {
	split := strings.Split(message, "#")
	pTerm, _ := strconv.Atoi(split[1])
	return pTerm
}
