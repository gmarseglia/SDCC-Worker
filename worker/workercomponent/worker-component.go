package worker

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"
	"worker/back"

	pb "github.com/gmarseglia/SDCC-Common/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	HostAddr       = flag.String("HostAddr", "", "The address of the host to advertise.")
	HostPort       = flag.String("HostPort", "", "The port of the host to advertise.")
	HostFullAddr   string
	MasterAddr     = flag.String("MasterAddr", "", "The address to connect to.")
	MasterPort     = flag.String("MasterPort", "", "The port of the master service.")
	MasterFullAddr string
	conn           *grpc.ClientConn
	c              pb.MasterClient
	PingTimeout    = time.Minute * 3
)

func dialServerAndSetClient() error {
	// Set up a connection to the gRPC server
	var err error

	if conn == nil {
		conn, err = grpc.Dial(MasterFullAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[Worker]: Could not Dial. More:\n%v", err)
			return err
		}
	}

	// create the client object
	if c == nil {
		c = pb.NewMasterClient(conn)
	}

	return nil
}

func NotifyWorkerActive(portChan chan string) error {
	// Dial master
	err := dialServerAndSetClient()
	if err != nil {
		return err
	}

	// Get port from back
	HostPort := <-portChan
	HostFullAddr = fmt.Sprintf("%s:%s", *HostAddr, HostPort)

	// Save workerAddr
	log.Printf("[Worker]: Address to be advertised to master: %s\n", HostFullAddr)

	// create the context
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()

	pingRequest := &pb.PingRequest{
		WorkerAddress: HostFullAddr,
	}

	// contact the server
	r, err := c.NotifyPing(ctx, pingRequest)

	if err != nil {
		log.Printf("[Worker]: Could not notify Master. More:\n%v", err)
		return err
	}

	// Print server response
	log.Printf("[Worker]: Server reply: %s", r.GetResult())

	if r.GetResult() == "ALREADY ADDED" {
		return errors.New("worker with same full address already added")
	}

	return nil
}

func PingServer(timeout int, usageCPU float32, pingType pb.PingType) error {
	// Dial master
	err := dialServerAndSetClient()
	if err != nil {
		return err
	}

	// create the context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout)*4)
	defer cancel()

	pingRequest := &pb.PingRequest{
		WorkerAddress: HostFullAddr,
		QueueSize:     int32(back.Active),
		UsageCPU:      usageCPU,
		Type:          pingType,
	}

	// contact the server
	reply, err := c.NotifyPing(ctx, pingRequest)

	if err != nil {
		log.Printf("[Worker]: Could not ping Master. More:\n%v", err)
	}

	if reply.GetResult() != "PING OK" {
		log.Printf("[Worker]: Server reply: %s", reply.GetResult())
	}

	return err
}
