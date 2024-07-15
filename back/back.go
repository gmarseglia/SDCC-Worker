package back

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"

	pb "github.com/gmarseglia/SDCC-Common/proto"
	"github.com/gmarseglia/SDCC-Common/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	HostPort           string = ""
	FakeCPUScale       float64
	Active             int
	ActiveChannel      = make(chan int, 1000)
	activeRequests     = make(map[CompositeID]bool)
	activeRequestsLock sync.RWMutex
	s                  *grpc.Server
)

type backServer struct {
	pb.UnimplementedBackServer
}

type CompositeID struct {
	ID      int32
	InnerID int32
}

// BackServer implementation, cancel the request given the compositeID
func (s *backServer) Cancel(ctx context.Context, in *pb.CancelRequest) (*pb.CancelReply, error) {
	log.Printf("[Back server]: Cancel %d.%d", in.GetID(), in.GetInnerID())

	// Check if the request is still exists
	compositeID := CompositeID{ID: in.GetID(), InnerID: in.GetInnerID()}
	activeRequestsLock.RLock()
	_, exists := activeRequests[compositeID]
	activeRequestsLock.RUnlock()

	// Cancel the request if it exists
	if exists {
		activeRequestsLock.Lock()
		activeRequests[compositeID] = false
		activeRequestsLock.Unlock()
	}

	return &pb.CancelReply{Result: "OK"}, nil
}

// Check if the request is still active
func checkStillActive(compositeID CompositeID) bool {
	activeRequestsLock.RLock()
	defer activeRequestsLock.RUnlock()
	return activeRequests[compositeID]
}

func applyKernel(input [][]float32, kernel [][]float32, useSigmoid bool) [][]float32 {
	// Get dimensions
	inputHeight := len(input)
	inputWidth := len(input[0])

	kernelWidth := len(kernel)
	kernelHeight := len(kernel[0])

	padTop := int(math.Floor((float64(kernelHeight) - 1.0) / 2.0))
	padBottom := int(math.Ceil((float64(kernelHeight) - 1.0) / 2.0))
	padLeft := int(math.Floor((float64(kernelWidth) - 1.0) / 2.0))
	padRight := int(math.Ceil((float64(kernelWidth) - 1.0) / 2.0))

	paddedHeight := padTop + inputHeight + padBottom
	paddedWidth := padLeft + inputHeight + padRight

	// Deep copy + padding
	padded := utils.GenerateEmptyMatrix(paddedHeight, paddedWidth)
	for i := 0; i < paddedHeight; i++ {
		for j := 0; j < paddedWidth; j++ {
			if (i < padTop) || (i >= padTop+inputHeight) || (j < padLeft) || (j >= padLeft+inputWidth) {
				padded[i][j] = 0
			} else {
				padded[i][j] = input[i-padTop][j-padLeft]
			}
		}
	}

	// Apply convolution + sigmoid
	convoluted := utils.GenerateEmptyMatrix(inputHeight, inputWidth)
	for i := 0; i < inputHeight; i++ {
		for j := 0; j < inputWidth; j++ {
			var newTarget float32 = 0
			for kw := 0; kw < kernelHeight; kw++ {
				for kh := 0; kh < kernelWidth; kh++ {
					newTarget += padded[i+kh][j+kw] * kernel[kh][kw]
				}
			}
			if useSigmoid {
				// Apply sigmoid
				convoluted[i][j] = float32(1.0 / (1.0 + math.Exp(float64(-newTarget))))
			} else {
				convoluted[i][j] = newTarget
			}
		}
	}

	return convoluted
}

func applyAvgPool(input [][]float32, poolSize int) [][]float32 {
	// Get dimensions
	inputHeight := len(input)
	inputWidth := len(input[0])
	actualPoolSize := min(poolSize, inputHeight, inputWidth)
	pooledHeight := int(inputHeight / actualPoolSize)
	pooledWidth := int(inputWidth / actualPoolSize)

	// Prepare result
	result := utils.GenerateEmptyMatrix(pooledHeight, pooledWidth)

	// Apply AvgPool
	for pi := 0; pi < pooledHeight; pi++ {
		for pj := 0; pj < pooledWidth; pj++ {
			var avgValue float32 = 0
			for i := 0; i < actualPoolSize; i++ {
				for j := 0; j < actualPoolSize; j++ {
					ti := pi*actualPoolSize + i
					tj := pj*actualPoolSize + j
					if ti < inputHeight && tj < inputWidth {
						avgValue += input[ti][tj] / float32(actualPoolSize*actualPoolSize)
					}
				}
			}
			result[pi][pj] = avgValue
		}
	}
	return result
}

// BackServer implementation, process the request of the convolutional layer
func (s *backServer) ConvolutionalLayer(ctx context.Context, in *pb.ConvolutionalLayerBackRequest) (*pb.ConvolutionalLayerBackReply, error) {
	// signal as active for logging purpose
	Active += 1
	ActiveChannel <- Active

	// Compute the composite ID
	compositeID := CompositeID{ID: in.GetID(), InnerID: in.GetInnerID()}

	// Set the request as active
	activeRequestsLock.Lock()
	activeRequests[compositeID] = true
	activeRequestsLock.Unlock()

	// Log
	log.Printf("[Back server]: Received  %d.%d", compositeID.ID, compositeID.InnerID)

	// Prepare reply
	backReply := &pb.ConvolutionalLayerBackReply{ID: in.GetID(), InnerID: in.GetInnerID()}

	// Allocate space for the results
	var results [][][]float32
	input := utils.ProtoToMatrix(in.Target)

	// Begin timing
	start := time.Now()

	// Apply the kernels if needed
	if in.GetUseKernels() {
		results = make([][][]float32, len(in.GetKernel()))
		for i, kernelProto := range in.GetKernel() {
			kernel := utils.ProtoToMatrix(kernelProto)
			results[i] = applyKernel(input, kernel, in.GetUseSigmoid())

			// Stop computation if cancelled
			if !checkStillActive(compositeID) {
				Active -= 1
				ActiveChannel <- Active
				return nil, status.Error(codes.Canceled, fmt.Sprintf("Request %d.%d cancelled as requested", compositeID.ID, compositeID.InnerID))
			}
		}
	} else {
		results = make([][][]float32, 1)
		results[0] = input
	}

	// Stop computation if cancelled
	if !checkStillActive(compositeID) {
		Active -= 1
		ActiveChannel <- Active
		return nil, status.Error(codes.Canceled, fmt.Sprintf("Request %d.%d cancelled as requested", compositeID.ID, compositeID.InnerID))
	}

	// Apply AvgPool if needed
	poolSize := int(in.GetAvgPoolSize())
	for _, result := range results {
		if poolSize > 1 {
			finalResult := applyAvgPool(result, poolSize)
			backReply.Result = append(backReply.Result, utils.MatrixToProto(finalResult))
		} else {
			backReply.Result = append(backReply.Result, utils.MatrixToProto(result))
		}

		// Stop computation if cancelled
		if !checkStillActive(compositeID) {
			Active -= 1
			ActiveChannel <- Active
			return nil, status.Error(codes.Canceled, fmt.Sprintf("Request %d.%d cancelled as requested", compositeID.ID, compositeID.InnerID))
		}
	}

	// Fake delay by CPU
	time.Sleep(time.Duration(time.Since(start).Seconds()*(FakeCPUScale-1)) * time.Second)

	// Stop computation if cancelled
	if !checkStillActive(compositeID) {
		Active -= 1
		ActiveChannel <- Active
		return nil, status.Error(codes.Canceled, fmt.Sprintf("Request %d.%d cancelled as requested", compositeID.ID, compositeID.InnerID))
	}

	activeRequestsLock.Lock()
	delete(activeRequests, compositeID)
	activeRequestsLock.Unlock()

	// signal as deactivated
	Active -= 1
	ActiveChannel <- Active

	log.Printf("[Back server]: Completed %d.%d", compositeID.ID, compositeID.InnerID)

	// send response
	return backReply, nil
}

func debugActive() {
	for Active := range ActiveChannel {
		log.Printf("[Back server]: Active rpc: %d", Active)
	}
}

func listen() (net.Listener, error) {
	// listen to request to a free port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", HostPort))
	if err != nil {
		return nil, err
	}

	return lis, nil
}

func StartServer(portChan chan string) error {
	// Listen
	workerListener, err := listen()
	if err != nil {
		log.Printf("[Back server]: Failed to listen.\nMore: %v", err)
		return err
	}

	HostPort := fmt.Sprintf("%v", workerListener.Addr().(*net.TCPAddr).Port)
	log.Printf("[Back server]: Back server listening at port: %s", HostPort)

	// Comunicate port to worker-component
	portChan <- HostPort

	// create a new server
	s = grpc.NewServer()

	// register the server
	pb.RegisterBackServer(s, &backServer{})

	// start debugging Active level
	go debugActive()

	// serve the request
	if err := s.Serve(workerListener); err != nil {
		log.Fatalf("[Back server]: failed to serve: %v", err)
	}

	return nil
}

func StopServer(wg *sync.WaitGroup) {
	log.Printf("[Back server]: Grafecully stopping...")

	// Graceful stop
	s.GracefulStop()
	log.Printf("[Back server]: Done.")

	// Comunicate on channel so sync
	(*wg).Done()
}
