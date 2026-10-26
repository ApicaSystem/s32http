package main

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/johannesboyne/gofakes3/backend/s3afero"
	"github.com/johannesboyne/gofakes3/backend/s3bolt"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	gofakes3 "github.com/logiqai/s32http"
	"github.com/logiqai/s32http/backend/logiq"
	"github.com/spf13/afero"
)

const usage = `
`

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

type fakeS3Flags struct {
	host          string
	backendKind   string
	initialBucket string
	fixedTimeStr  string
	noIntegrity   bool
	hostBucket    bool
	autoBucket    bool
	quiet         bool

	boltDb         string
	directFsPath   string
	directFsMeta   string
	directFsBucket string
	fsPath         string
	fsMeta         string

	debugCPU  string
	debugHost string

	logiqHost                 string
	logiqPort                 string
	maxBatch                  int
	maxWorkers                int
	maxObjectPropagationCount int
	logiqLogLevel             string
}

func (f *fakeS3Flags) attach(flagSet *flag.FlagSet) {
	flagSet.StringVar(&f.host, "host", ":9000", "Host to run the service")
	flagSet.StringVar(&f.fixedTimeStr, "time", "", "RFC3339 format. If passed, the server's clock will always see this time; does not affect existing stored dates.")
	flagSet.StringVar(&f.initialBucket, "initialbucket", "", "If passed, this bucket will be created on startup if it does not already exist.")
	flagSet.BoolVar(&f.noIntegrity, "no-integrity", false, "Pass this flag to disable Content-MD5 validation when uploading.")
	flagSet.BoolVar(&f.hostBucket, "hostbucket", false, "If passed, the bucket name will be extracted from the first segment of the hostname, rather than the first part of the URL path.")
	flagSet.BoolVar(&f.autoBucket, "autobucket", false, "If passed, nonexistent buckets will be created on first use instead of raising an error")

	// Logging
	flagSet.BoolVar(&f.quiet, "quiet", false, "If passed, log messages are not printed to stderr")

	// Backend specific:
	flagSet.StringVar(&f.backendKind, "backend", "", "Backend to use to store data (memory, bolt, directfs, fs, logiq)")
	flagSet.StringVar(&f.boltDb, "bolt.db", "locals3.db", "Database path / name when using bolt backend")
	flagSet.StringVar(&f.directFsPath, "directfs.path", "", "File path to serve using S3. You should not modify the contents of this path outside gofakes3 while it is running as it can cause inconsistencies.")
	flagSet.StringVar(&f.directFsMeta, "directfs.meta", "", "Optional path for storing S3 metadata for your bucket. If not passed, metadata will not persist between restarts of gofakes3.")
	flagSet.StringVar(&f.directFsBucket, "directfs.bucket", "mybucket", "Name of the bucket for your file path; this will be the only supported bucket by the 'directfs' backend for the duration of your run.")
	flagSet.StringVar(&f.fsPath, "fs.path", "", "Path to your S3 buckets. Buckets are stored under the '/buckets' subpath.")
	flagSet.StringVar(&f.fsMeta, "fs.meta", "", "Optional path for storing S3 metadata for your buckets. Defaults to the '/metadata' subfolder of -fs.path if not passed.")
	flagSet.IntVar(&f.maxBatch, "logiq.maxbatch", 25, "Maximum number of messages to batch before sending to Logiq")
	flagSet.IntVar(&f.maxWorkers, "logiq.maxworkers", 100, "Maximum number of workers to use to send messages to Logiq")
	flagSet.IntVar(&f.maxObjectPropagationCount, "apica.maxobjectpropagationcount", 1, "Maximum number of objects proxyable to apica in parallel")

	flagSet.StringVar(&f.logiqHost, "logiq.host", "logiq-flash", "Logiq host to send messages to")
	flagSet.StringVar(&f.logiqPort, "logiq.port", "9999", "Logiq port to send messages to")
	flagSet.StringVar(&f.logiqLogLevel, "logiq.loglevel", "info", "Log level to use for Logiq messages")

	// Debugging:
	flagSet.StringVar(&f.debugHost, "debug.host", "", "Run the debug server on this host")
	flagSet.StringVar(&f.debugCPU, "debug.cpu", "", "Create CPU profile in this file")

	// Deprecated:
	flagSet.StringVar(&f.boltDb, "db", "locals3.db", "Deprecated; use -bolt.db")
	flagSet.StringVar(&f.initialBucket, "bucket", "", `Deprecated; use -initialbucket`)
}

func (f *fakeS3Flags) timeOptions() (source gofakes3.TimeSource, skewLimit time.Duration, err error) {
	skewLimit = gofakes3.DefaultSkewLimit

	if f.fixedTimeStr != "" {
		fixedTime, err := time.Parse(time.RFC3339Nano, f.fixedTimeStr)
		if err != nil {
			return nil, 0, err
		}
		source = gofakes3.FixedTimeSource(fixedTime)
		skewLimit = 0
	}

	return source, skewLimit, nil
}

func debugServer(host string) {
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)

	srv := &http.Server{Addr: host}
	srv.Handler = mux
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

type apicaEnv struct {
	maxBatch                  int
	maxWorkers                int
	maxObjectPropagationCount int
	logiqHost                 string
	logiqPort                 string
	logiqLogLevel             string
}

func convertToValidInt(input string, defaultValue int) int {
	if input == "" {
		return defaultValue
	}
	intValue, err := strconv.Atoi(input)
	if err != nil {
		log.Printf("\ninvalid integer env value passed: %s. Error: %v\n", input, err)
		return defaultValue
	}
	if intValue <= 0 {
		return defaultValue
	}
	return intValue
}

var DEFAULT_MAX_BATCH_SIZE = 25
var DEFAULT_MAX_WORKERS_COUNT = 100
var DEFAULT_MAX_OBJECT_PROPAGATION_COUNT = 1
var DEFAULT_LOG_LEVEL = "info"
var DEFAULT_LOGIQ_HOST = "logiq-flash"
var DEFAULT_LOGIQ_PORT = "9999"

func readEnv() *apicaEnv {
	env := &apicaEnv{}
	s := os.Getenv("apica_s3_max_batch")
	env.maxBatch = convertToValidInt(s, DEFAULT_MAX_BATCH_SIZE)

	maxWorkers := os.Getenv("apica_s3_max_workers")
	env.maxWorkers = convertToValidInt(maxWorkers, DEFAULT_MAX_WORKERS_COUNT)

	maxObjectPropagationCount := os.Getenv("apica_s3_max_object_propagation_count")
	env.maxObjectPropagationCount = convertToValidInt(maxObjectPropagationCount, DEFAULT_MAX_OBJECT_PROPAGATION_COUNT)

	s = os.Getenv("apica_s3_logiq_host")
	if s == "" {
		env.logiqHost = DEFAULT_LOGIQ_HOST
	} else {
		env.logiqHost = s
	}

	s = os.Getenv("apica_s3_logiq_port")
	if s == "" {
		env.logiqPort = DEFAULT_LOGIQ_PORT
	} else {
		env.logiqPort = s
	}

	s = os.Getenv("apica_s3_log_level")
	if s == "" {
		env.logiqLogLevel = DEFAULT_LOG_LEVEL
	} else {
		env.logiqLogLevel = s
	}

	return env
}

func run() error {
	var values fakeS3Flags

	flagSet := flag.NewFlagSet("", 0)
	values.attach(flagSet)

	env := readEnv()

	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return err
	}

	stopper, err := profile(values)
	if err != nil {
		return err
	}
	defer stopper()

	if values.debugHost != "" {
		log.Println("starting debug server at", fmt.Sprintf("http://%s/debug/pprof", values.debugHost))
		go debugServer(values.debugHost)
	}

	var backend gofakes3.Backend

	timeSource, timeSkewLimit, err := values.timeOptions()
	if err != nil {
		return err
	}

	switch values.backendKind {
	case "":
		flag.PrintDefaults()
		fmt.Println()
		return fmt.Errorf("-backend is required")

	case "bolt":
		var err error
		backend, err = s3bolt.NewFile(values.boltDb, s3bolt.WithTimeSource(timeSource))
		if err != nil {
			return err
		}
		log.Println("using bolt backend with file", values.boltDb)

	case "mem", "memory":
		if values.initialBucket == "" {
			log.Println("no buckets available; consider passing -initialbucket")
		}
		backend = s3mem.New(s3mem.WithTimeSource(timeSource))
		log.Println("using memory backend")

	case "logiq", "lq":
		if values.initialBucket == "" {
			log.Println("no buckets available; consider passing -initialbucket")
		}
		backend = logiq.New(
			logiq.WithLogLevel(env.logiqLogLevel),
			logiq.WithLogiqHost(env.logiqHost),
			logiq.WithLogiqPort(env.logiqPort),
			logiq.WithMaxBatch(env.maxBatch),
			logiq.WithMaxWorkers(env.maxWorkers),
			logiq.WithMaxObjectPropagationCount(env.maxObjectPropagationCount),
			logiq.WithTimeSource(timeSource))
		log.Println("using logiq backend")

	case "fs":
		if timeSource != nil {
			log.Println("warning: time source not supported by this backend")
		}

		baseFs, err := s3afero.FsPath(values.fsPath)
		if err != nil {
			return fmt.Errorf("gofakes3: could not create -fs.path: %v", err)
		}

		var options []s3afero.MultiOption
		if values.fsMeta != "" {
			metaFs, err := s3afero.FsPath(values.fsMeta)
			if err != nil {
				return fmt.Errorf("gofakes3: could not create -fs.meta: %v", err)
			}
			options = append(options, s3afero.MultiWithMetaFs(metaFs))
		}

		backend, err = s3afero.MultiBucket(baseFs, options...)
		if err != nil {
			return err
		}

	case "directfs":
		if values.initialBucket != "" {
			return fmt.Errorf("gofakes3: -initialbucket not supported by directfs")
		}
		if values.autoBucket {
			return fmt.Errorf("gofakes3: -autobucket not supported by directfs")
		}
		if timeSource != nil {
			log.Println("warning: time source not supported by this backend")
		}

		baseFs, err := s3afero.FsPath(values.directFsPath)
		if err != nil {
			return fmt.Errorf("gofakes3: could not create -directfs.path: %v", err)
		}

		var metaFs afero.Fs
		if values.directFsMeta != "" {
			metaFs, err = s3afero.FsPath(values.directFsMeta)
			if err != nil {
				return fmt.Errorf("gofakes3: could not create -directfs.meta: %v", err)
			}
		} else {
			log.Println("using ephemeral memory backend for metadata; this will not persist. See -directfs.metapath flag if you need persistence.")
		}

		backend, err = s3afero.SingleBucket(values.directFsBucket, baseFs, metaFs)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown backend %q", values.backendKind)
	}

	if values.initialBucket != "" {
		if err := backend.CreateBucket(values.initialBucket); err != nil && !gofakes3.IsAlreadyExists(err) {
			return fmt.Errorf("gofakes3: could not create initial bucket %q: %v", values.initialBucket, err)
		}
		log.Println("created -initialbucket", values.initialBucket)
	}

	logger := gofakes3.GlobalLog()
	if values.quiet {
		logger = gofakes3.DiscardLog()
	}

	faker := gofakes3.New(backend,
		gofakes3.WithIntegrityCheck(!values.noIntegrity),
		gofakes3.WithTimeSkewLimit(timeSkewLimit),
		gofakes3.WithTimeSource(timeSource),
		gofakes3.WithLogger(logger),
		gofakes3.WithHostBucket(values.hostBucket),
		gofakes3.WithAutoBucket(values.autoBucket),
	)

	return listenAndServe(values.host, faker.Server())
}

func listenAndServe(addr string, handler http.Handler) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	log.Println("using port:", listener.Addr().(*net.TCPAddr).Port)
	server := &http.Server{Addr: addr, Handler: handler}

	return server.Serve(listener)
}

func profile(values fakeS3Flags) (func(), error) {
	fn := func() {}

	if values.debugCPU != "" {
		f, err := os.Create(values.debugCPU)
		if err != nil {
			return fn, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return fn, err
		}
		return pprof.StopCPUProfile, nil
	}

	return fn, nil
}
