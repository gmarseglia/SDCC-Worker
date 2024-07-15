package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"worker/back"
	worker "worker/workercomponent"

	pb "github.com/gmarseglia/SDCC-Common/proto"
	"github.com/gmarseglia/SDCC-Common/utils"

	"github.com/shirou/gopsutil/cpu"
)

var (
	PingTimeout  = flag.Int("PingTimeout", 1, "The ping timeout in seconds.")
	FakeCPUScale = flag.Float64("FakeCPUScale", 1, "The scale of the fake CPU usage.")
	wg           = sync.WaitGroup{}
)

func setupFields() {
	// Setup WorkerPort
	utils.SetupFieldOptional(worker.HostAddr, "HostAddr", utils.GetOutboundIP().String())
	utils.SetupFieldOptional(worker.HostPort, "HostPort", "0")
	utils.SetupFieldMandatory(worker.MasterAddr, "MasterAddr", func() {
		log.Printf("[Main]: MasterAddr is a mandatory field.")
		exit()
	})
	utils.SetupFieldOptional(worker.MasterPort, "MasterPort", "55556")
	worker.MasterFullAddr = fmt.Sprintf("%s:%s", *worker.MasterAddr, *worker.MasterPort)
	back.HostPort = *worker.HostPort
	back.FakeCPUScale = *FakeCPUScale
}

func stopComponentsAndExit(message string) {
	log.Printf("[Main]: %s. Begin components stop.", message)

	go func() {
		worker.PingServer(10, 0, pb.PingType_DEACTIVATE)
	}()
	time.Sleep(time.Millisecond * 100)

	back.StopServer(&wg)

	wg.Wait()

	exit()
}

func exit() {
	log.Printf("[Main]: All components stopped. Main component stopped. Goodbye.")

	os.Exit(0)
}

func main() {
	flag.Parse()

	log.Printf("[Main]: Welcome. Main component started. Begin components start.")

	setupFields()

	// Channel to comunicate the host port
	portChan := make(chan string)

	// Activate the Back Server
	wg.Add(1)
	go func() {
		err := back.StartServer(portChan)
		if err != nil {
			exit()
		}
	}()
	time.Sleep(time.Millisecond * 10)

	// install signal handler
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		stopComponentsAndExit("SIGTERM received")
	}()

	// notify master
	tries := 0
	for {
		err := worker.NotifyWorkerActive(portChan)
		if err != nil {
			tries += 1
			time.Sleep(time.Second * time.Duration(2*tries))
			if tries > 7 {
				stopComponentsAndExit("Master unreachable")
			}
		} else {
			break
		}
	}

	// infite loop with server pings
	for {
		// Prepare the request
		percentage := 0.0
		result, errCPU := cpu.Percent(time.Second, false)
		if errCPU != nil {
			percentage = -1.0
		} else {
			percentage = result[0]
		}
		err := worker.PingServer(*PingTimeout*10, float32(percentage*(*FakeCPUScale)), pb.PingType_PING)
		if err != nil {
			stopComponentsAndExit("Ping failed")
		}
		time.Sleep(time.Second * time.Duration(*PingTimeout-1))
	}

}
