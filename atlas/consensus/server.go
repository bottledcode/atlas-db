package consensus

type Server struct {
	UnimplementedConsensusServer
	tableNodeCounts map[string]map[string]uint
}
