package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"
	gen "github.com/twitchscience/gologging/key_name_generator"
)

const targetBucket = "twlogger-test"

var rps = flag.Int("rps", 1000, "requests / sec")

type stdoutNotifier struct{}
type stderrNotifier struct{}

type localInstanceFetcher struct{}

func (s *stdoutNotifier) SendMessage(r *uploader.UploadReceipt) error {
	fmt.Println(r.KeyName)
	return nil
}
func (s *stderrNotifier) SendError(e error) {
	log.Println(e)
}

func (f *localInstanceFetcher) GetHost() string {
	return "basic-host"
}

func (f *localInstanceFetcher) GetClusterName() string {
	return "basic-test"
}

func (f *localInstanceFetcher) GetAutoScaleGroup() string {
	return "basic-asg"
}

const (
	MAX_LINES_PER_LOG = 10000 // 10 thousand requests.
	// At 20k rps and 9 nodes we will rotate about every 10 mins.
	JITTER = 20
)

var (
	// Add jitter to the max lines. Draw a random number distributed normally with std deviation mlpl/2000.
	adjustedMaxLines = int(MAX_LINES_PER_LOG + rand.New(rand.NewSource(time.Now().UnixNano())).NormFloat64()*float64(JITTER))
)

func main() {
	flag.Parse()

	info := gen.BuildInstanceInfo(&localInstanceFetcher{}, "basic_example", ".")
	rotateCoordinator := gologging.NewRotateCoordinator(adjustedMaxLines, time.Hour*1)
	uploaderBuilder := &uploader.S3UploaderBuilder{
		Bucket: targetBucket,
		KeyNameGenerator: &gen.EdgeKeyNameGenerator{
			Info: info,
		},
		S3Manager: s3manager.NewUploader(session.New()),
	}
	logger, err := gologging.StartS3Logger(
		rotateCoordinator,
		info,
		&stdoutNotifier{},
		uploaderBuilder,
		&stderrNotifier{},
		5,
	)
	if err != nil {
		log.Fatalf("Error building uploader: %s\n ", err)
	}

	i := 0
	now := time.Now()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sigc
		logger.Close()
		fmt.Printf("Produced %f rps\n", float64(i)/(float64(time.Now().Sub(now))/float64(time.Second)))
		os.Exit(0)
	}()
	x := int(time.Second) / *rps
	for ; i < MAX_LINES_PER_LOG*4; i++ {
		// throttle - there is a better solution to this
		defer func() {
			if x := recover(); x != nil { // means we cuagh a signal
				time.Sleep(120 * time.Second)
			}
		}()
		time.Sleep(time.Duration(int(0.8 * float64(x))))
		logger.Logf("MOAR! %d", i)
	}
	logger.Close()
}
