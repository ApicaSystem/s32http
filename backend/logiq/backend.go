package logiq

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	gofakes3 "github.com/logiqai/s32http"
	"github.com/logiqai/s32http/internal/goskipiter"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	emptyPrefix       = &gofakes3.Prefix{}
	emptyVersionsPage = &gofakes3.ListBucketVersionsPage{}
	// Create a new instance of the logger. You can have any number of instances.
	log = logrus.New()
)

type Backend struct {
	buckets          map[string]*bucket
	timeSource       gofakes3.TimeSource
	versionGenerator *versionGenerator
	versionSeed      int64
	versionSeedSet   bool
	versionScratch   []byte
	lock             sync.RWMutex
	logiqHost        string
	logiqPort        string
	maxWorkers       int
	maxBatch         int
	retryBudget      time.Duration
	attemptTimeout   time.Duration
	minAttempt       time.Duration
	backoffBase      time.Duration
	backoffMax       time.Duration
	delivered        *ledger
}

// Splunk's S3 client abandons a PUT that has had no response for 60s and
// retries it, so every PUT is answered within this budget.
const defaultRetryBudget = 45 * time.Second

// Flash asks for 1-5s; a longer Retry-After is honoured only up to this.
const maxRetryAfter = 10 * time.Second

var _ gofakes3.Backend = &Backend{}
var _ gofakes3.VersionedBackend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource gofakes3.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func WithMaxWorkers(maxWorkers int) Option {
	log.Infoln("Setting max workers to ", maxWorkers)
	return func(b *Backend) { b.maxWorkers = maxWorkers }
}

func WithMaxBatch(maxBatch int) Option {
	log.Infoln("Setting max batch to ", maxBatch)
	return func(b *Backend) { b.maxBatch = maxBatch }
}

func WithLogiqHost(host string) Option {
	log.Infoln("Setting logiq host to ", host)
	return func(b *Backend) { b.logiqHost = host }
}

func WithLogiqPort(port string) Option {
	log.Infoln("Setting logiq port to ", port)
	return func(b *Backend) { b.logiqPort = port }
}

func WithLogLevel(level string) Option {
	log.Infoln("Setting loglevel to", level)
	return func(b *Backend) {
		if strings.ToLower(level) == "debug" {
			log.SetLevel(logrus.DebugLevel)
		}
		if strings.ToLower(level) == "info" {
			log.SetLevel(logrus.InfoLevel)
		}
		if strings.ToLower(level) == "warn" {
			log.SetLevel(logrus.WarnLevel)
		}
		if strings.ToLower(level) == "error" {
			log.SetLevel(logrus.ErrorLevel)
		}
		if strings.ToLower(level) == "fatal" {
			log.SetLevel(logrus.FatalLevel)
		}
	}
}

func WithVersionSeed(seed int64) Option {
	return func(b *Backend) { b.versionSeed = seed; b.versionSeedSet = true }
}

func New(opts ...Option) *Backend {
	b := &Backend{
		buckets:        make(map[string]*bucket),
		retryBudget:    defaultRetryBudget,
		attemptTimeout: 20 * time.Second,
		backoffBase:    250 * time.Millisecond,
		backoffMax:     4 * time.Second,
		// An attempt cut off by the deadline may still be accepted by logiq,
		// and Splunk's retry would then send it again.
		minAttempt: 5 * time.Second,
		// Splunk retries a failed object at most about 6 minutes apart.
		delivered: newLedger(15*time.Minute, 50000),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = gofakes3.DefaultTimeSource()
	}
	if b.versionGenerator == nil {
		if b.versionSeedSet {
			b.versionGenerator = newVersionGenerator(uint64(b.versionSeed), 0)
		} else {
			b.versionGenerator = newVersionGenerator(uint64(b.timeSource.Now().UnixNano()), 0)
		}
	}
	return b
}

func (db *Backend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var buckets = make([]gofakes3.BucketInfo, 0, len(db.buckets))
	for _, bucket := range db.buckets {
		buckets = append(buckets, gofakes3.BucketInfo{
			Name:         bucket.name,
			CreationDate: bucket.creationDate,
		})
	}

	return buckets, nil
}

func (db *Backend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}

	db.lock.RLock()
	defer db.lock.RUnlock()

	storedBucket := db.buckets[name]
	if storedBucket == nil {
		return nil, gofakes3.BucketNotFound(name)
	}

	var response = gofakes3.NewObjectList()
	var iter = goskipiter.New(storedBucket.objects.Iterator())
	var match gofakes3.PrefixMatch

	if page.Marker != "" {
		iter.Seek(page.Marker)
		iter.Next() // Move to the next item after the Marker
	}

	var cnt int64 = 0

	var lastMatchedPart string

	for iter.Next() {
		item := iter.Value().(*bucketObject)

		if !prefix.Match(item.data.name, &match) {
			continue

		} else if match.CommonPrefix {
			if match.MatchedPart == lastMatchedPart {
				continue // Should not count towards keys
			}
			response.AddPrefix(match.MatchedPart)
			lastMatchedPart = match.MatchedPart

		} else {
			response.Add(&gofakes3.Content{
				Key:          item.data.name,
				LastModified: gofakes3.NewContentTime(item.data.lastModified),
				ETag:         `"` + hex.EncodeToString(item.data.hash) + `"`,
				Size:         int64(len(item.data.body)),
			})
		}

		cnt++
		if page.MaxKeys > 0 && cnt >= page.MaxKeys {
			response.NextMarker = item.data.name
			response.IsTruncated = iter.Next()
			break
		}
	}

	return response, nil
}

func (db *Backend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] != nil {
		return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
	}

	db.buckets[name] = newBucket(name, db.timeSource.Now(), db.nextVersion)
	return nil
}

func (db *Backend) DeleteBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] == nil {
		return gofakes3.ErrNoSuchBucket
	}

	if db.buckets[name].objects.Len() > 0 {
		return gofakes3.ResourceError(gofakes3.ErrBucketNotEmpty, name)
	}

	delete(db.buckets, name)

	return nil
}

func (db *Backend) BucketExists(name string) (exists bool, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.buckets[name] != nil, nil
}

func (db *Backend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil || obj.data.deleteMarker {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	return obj.data.toObject(nil, false)
}

func (db *Backend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil || obj.data.deleteMarker {
		// FIXME: If the current version of the object is a delete marker,
		// Amazon S3 behaves as if the object was deleted and includes
		// x-amz-delete-marker: true in the response.
		//
		// The solution may be to return an object but no error if the object is
		// a delete marker, and let the main GoFakeS3 class decide what to do.
		return nil, gofakes3.KeyNotFound(objectName)
	}

	result, err := obj.data.toObject(rangeRequest, true)
	if err != nil {
		return nil, err
	}

	if bucket.versioning != gofakes3.VersioningEnabled {
		result.VersionID = ""
	}

	return result, nil
}

func (db *Backend) PutObjectOrig(bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (result gofakes3.PutObjectResult, err error) {
	// No need to lock the backend while we read the data into memory; it holds
	// the write lock open unnecessarily, and could be blocked for an unreasonably
	// long time by a connection timing out:
	bts, err := gofakes3.ReadAll(input, size)
	if err != nil {
		return result, err
	}

	err = gofakes3.MergeMetadata(db, bucketName, objectName, meta)
	if err != nil {
		return result, err
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	hash := md5.Sum(bts)

	item := &bucketData{
		name:         objectName,
		body:         bts,
		hash:         hash[:],
		etag:         `"` + hex.EncodeToString(hash[:]) + `"`,
		metadata:     meta,
		lastModified: db.timeSource.Now(),
	}

	bucket.put(objectName, item)

	if bucket.versioning == gofakes3.VersioningEnabled {
		// versionID is assigned in bucket.put()
		result.VersionID = item.versionID
	}

	return result, nil
}

// SendToLogiq sends the events of an S3 object to logiq in batches, retrying
// failed batches until deadline. key names the object and its content:
// batches already accepted for the same key are not sent again.
func (db *Backend) SendToLogiq(key string, bts []byte, deadline time.Time) error {
	m := []map[string]interface{}{}
	err := json.Unmarshal(bts, &m)
	if err != nil {
		return err
	}
	nBatches := (len(m) + db.maxBatch - 1) / db.maxBatch
	entry := db.delivered.acquire(key, db.maxBatch, nBatches, db.timeSource.Now())
	defer func() { db.delivered.release(entry, db.timeSource.Now()) }()
	pending := 0
	for _, done := range entry.done {
		if !done {
			pending++
		}
	}
	if pending < nBatches {
		log.Infof("%s: %d of %d batches already delivered, sending %d", key, nBatches-pending, nBatches, pending)
	}

	send := &objectSend{db: db, key: key, nBatches: nBatches, deadline: deadline}
	send.stop, send.abort = context.WithCancel(context.Background())
	defer send.abort()

	ch := make(chan bool, db.maxWorkers)
	wg := sync.WaitGroup{}
	errs := make([]error, nBatches)
	sizes := make([]int, nBatches)
	cluster, clusterExists := os.LookupEnv("service_name")
	namespace := "s2s"
	if !clusterExists {
		cluster = "s2s-ingest"
	}
	for i := 0; i < len(m); i += db.maxBatch {
		j := i + db.maxBatch
		if j > len(m) {
			j = len(m)
		}
		batch := m[i:j]
		n := i / db.maxBatch
		sizes[n] = len(batch)
		if entry.done[n] {
			continue
		}
		for _, batchItem := range batch {
			batchItem["namespace"] = namespace
			batchItem["cluster_id"] = cluster
			batchItem["message"] = batchItem["event"]
			delete(batchItem, "event")
		}
		log.Debugln("Sending batch:", len(batch))

		b, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		ch <- true
		var skip error
		if send.stop.Err() != nil {
			skip = errors.New("not sent, logiq refused the object")
		} else if deadline.Sub(time.Now()) < db.minAttempt {
			skip = errors.New("not sent, out of time")
		}
		if skip != nil {
			<-ch
			errs[n] = skip
			continue
		}
		wg.Add(1)
		go func(wgPtr *sync.WaitGroup, index int, batchBytes []byte, doneCh chan bool) {
			defer func() {
				<-ch
				wgPtr.Done()
			}()
			errs[index] = send.batch(index, sizes[index], batchBytes)
		}(&wg, n, b, ch)
	}
	wg.Wait()
	failed, failedEvents := 0, 0
	var firstErr error
	for n, err := range errs {
		if err == nil {
			entry.done[n] = true
			continue
		}
		failed++
		failedEvents += sizes[n]
		if firstErr == nil {
			firstErr = err
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d batches (%d events) not delivered, first error: %v", failed, nBatches, failedEvents, firstErr)
	}
	return nil
}

// objectSend is one PUT's attempt at sending the batches of an object.
type objectSend struct {
	db       *Backend
	key      string
	nBatches int
	deadline time.Time
	// stop is cancelled when logiq refuses the object outright (bad token,
	// wrong route): the remaining batches are not sent and retries end.
	stop       context.Context
	abort      context.CancelFunc
	warnOnce   sync.Once
	refuseOnce sync.Once
}

// batch sends batch n, retrying 429, 5xx and transport errors until the
// deadline. A batch logiq can never accept (400, 413) is dropped and counts
// as delivered, so it does not hold up the object.
func (s *objectSend) batch(n, events int, bts []byte) error {
	desc := fmt.Sprintf("%s batch %d/%d (%d events)", s.key, n+1, s.nBatches, events)
	start := time.Now()
	lastResponse := ""
	for attempt := 1; ; attempt++ {
		attemptDeadline := time.Now().Add(s.db.attemptTimeout)
		if s.deadline.Before(attemptDeadline) {
			attemptDeadline = s.deadline
		}
		ctx, cancel := context.WithDeadline(context.Background(), attemptDeadline)
		status, body, retryAfter, err := s.db.SendToLogiqSingleBatch(ctx, bts)
		cancel()

		outcome := fmt.Sprintf("status %d, body %q", status, truncate(body, 1024))
		if err != nil {
			outcome = err.Error()
		} else {
			lastResponse = outcome
		}
		switch {
		case err == nil && status/100 == 2:
			return nil
		case err == nil && (status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge):
			log.Errorf("Dropping %s, logiq will never accept it: %s", desc, outcome)
			return nil
		case err == nil && status != http.StatusTooManyRequests && status/100 != 5:
			s.refuseOnce.Do(func() { log.Errorf("Not sending %s or the rest of the object: %s", desc, outcome) })
			s.abort()
			return errors.New(outcome)
		}

		log.Debugf("Attempt %d of %s failed: %s", attempt, desc, outcome)
		s.warnOnce.Do(func() { log.Warnf("Retrying %s: %s", desc, outcome) })
		wait := s.db.backoff(attempt, retryAfter)
		if time.Now().Add(wait + s.db.minAttempt).After(s.deadline) {
			// The last attempt may have been cut short by the deadline;
			// what logiq last answered is what explains the failure.
			if err != nil && lastResponse != "" {
				outcome += "; last response from logiq: " + lastResponse
			}
			// PutObject logs the failed PUT once, with this error.
			log.Debugf("Giving up on %s after %d attempts in %v: %s", desc, attempt, time.Since(start).Round(time.Millisecond), outcome)
			return errors.New(outcome)
		}
		select {
		case <-s.stop.Done():
			return errors.New(outcome)
		case <-time.After(wait):
		}
	}
}

// backoff is how long to wait before the next attempt: exponential with full
// jitter, and at least as long as logiq asked for in Retry-After.
func (db *Backend) backoff(attempt int, retryAfter time.Duration) time.Duration {
	ceiling := db.backoffBase
	for i := 1; i < attempt && ceiling < db.backoffMax; i++ {
		ceiling *= 2
	}
	if ceiling > db.backoffMax {
		ceiling = db.backoffMax
	}
	wait := time.Duration(rand.Int63n(int64(ceiling))) + 1
	if retryAfter > wait {
		wait = retryAfter
	}
	return wait
}

func parseRetryAfter(h http.Header) time.Duration {
	secs, err := strconv.Atoi(h.Get("Retry-After"))
	if err != nil || secs <= 0 {
		return 0
	}
	if d := time.Duration(secs) * time.Second; d < maxRetryAfter {
		return d
	}
	return maxRetryAfter
}

func truncate(b []byte, max int) string {
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}

// SendToLogiqSingleBatch makes one attempt at posting a batch and returns
// logiq's status, body and Retry-After, or the transport error.
func (db *Backend) SendToLogiqSingleBatch(ctx context.Context, bts []byte) (int, []byte, time.Duration, error) {
	logiqClient := &http.Client{}
	logiqURL := fmt.Sprintf("http://%s:%s/v1/json_batch", db.logiqHost, db.logiqPort)
	logiqClient.Timeout = time.Second * 60
	token, exists := os.LookupEnv("x_api_key")

	req, reqErr := http.NewRequestWithContext(ctx, "POST", logiqURL, bytes.NewBuffer(bts))
	if reqErr != nil {
		log.Errorf("Error creating request: %v", reqErr)
		return 0, nil, 0, reqErr
	}
	if exists {
		req.Header.Add("Content-type", "application/json")
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	} else {
		req.Header.Add("Content-type", "application/json")
	}
	resp, respErr := logiqClient.Do(req)
	if respErr != nil {
		return 0, nil, 0, respErr
	}
	defer resp.Body.Close()
	// The status decides the outcome: a 2xx whose body cannot be read was
	// still accepted, and retrying it would duplicate the batch.
	body, bodyErr := ioutil.ReadAll(resp.Body)
	if bodyErr != nil {
		log.Debugf("Error reading response from logiq: %v", bodyErr)
	} else {
		log.Debugf("Response from logiq: %v", string(body))
	}
	return resp.StatusCode, body, parseRetryAfter(resp.Header), nil
}

func (db *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (result gofakes3.PutObjectResult, err error) {
	// No need to lock the backend while we read the data into memory; it holds
	// the write lock open unnecessarily, and could be blocked for an unreasonably
	// long time by a connection timing out:
	bts, err := gofakes3.ReadAll(input, size)
	if err != nil {
		return result, err
	}
	// Splunk's 60s wait for our answer starts once it has sent the body, so
	// the budget runs from here, including any wait for the lock below.
	deadline := time.Now().Add(db.retryBudget)

	err = gofakes3.MergeMetadata(db, bucketName, objectName, meta)
	if err != nil {
		return result, err
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	hash := md5.Sum(bts)

	item := &bucketData{
		name:         objectName,
		body:         bts,
		hash:         hash[:],
		etag:         `"` + hex.EncodeToString(hash[:]) + `"`,
		metadata:     meta,
		lastModified: db.timeSource.Now(),
	}

	//
	// bucket.put(objectName, item)
	// We will hijack this and send over data to the logiq server

	retval := db.SendToLogiq(bucketName+"/"+objectName+"/"+hex.EncodeToString(hash[:]), bts, deadline)
	if retval != nil {
		log.Errorf("Error sending %s/%s to logiq, failing the PUT so Splunk retries it: %v", bucketName, objectName, retval)
		return result, retval
	}

	if bucket.versioning == gofakes3.VersioningEnabled {
		// versionID is assigned in bucket.put()
		result.VersionID = item.versionID
	}

	return result, nil
}

func (db *Backend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	return bucket.rm(objectName, db.timeSource.Now())
}

func (db *Backend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	now := db.timeSource.Now()

	for _, object := range objects {
		dresult, err := bucket.rm(object, now)
		_ = dresult // FIXME: what to do with rm result in multi delete?

		if err != nil {
			errres := gofakes3.ErrorResultFromError(err)
			if errres.Code == gofakes3.ErrInternal {
				// FIXME: log
			}

			result.Error = append(result.Error, errres)

		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: object,
			})
		}
	}

	return result, nil
}

func (db *Backend) VersioningConfiguration(bucketName string) (versioning gofakes3.VersioningConfiguration, rerr error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return versioning, gofakes3.BucketNotFound(bucketName)
	}

	versioning.Status = bucket.versioning

	return versioning, nil
}

func (db *Backend) SetVersioningConfiguration(bucketName string, v gofakes3.VersioningConfiguration) error {
	if v.MFADelete.Enabled() {
		return gofakes3.ErrNotImplemented
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return gofakes3.BucketNotFound(bucketName)
	}

	bucket.setVersioning(v.Enabled())

	return nil
}

func (db *Backend) GetObjectVersion(
	bucketName, objectName string,
	versionID gofakes3.VersionID,
	rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {

	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	ver, err := bucket.objectVersion(objectName, versionID)
	if err != nil {
		return nil, err
	}

	return ver.toObject(rangeRequest, true)
}

func (db *Backend) HeadObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (*gofakes3.Object, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	ver, err := bucket.objectVersion(objectName, versionID)
	if err != nil {
		return nil, err
	}

	return ver.toObject(nil, false)
}

func (db *Backend) DeleteObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	return bucket.rmVersion(objectName, versionID, db.timeSource.Now())
}

func (db *Backend) ListBucketVersions(
	bucketName string,
	prefix *gofakes3.Prefix,
	page *gofakes3.ListBucketVersionsPage,
) (*gofakes3.ListBucketVersionsResult, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}
	if page == nil {
		page = emptyVersionsPage
	}

	db.lock.RLock()
	defer db.lock.RUnlock()

	result := gofakes3.NewListBucketVersionsResult(bucketName, prefix, page)

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	var iter = goskipiter.New(bucket.objects.Iterator())
	var match gofakes3.PrefixMatch

	if page.KeyMarker != "" {
		if !prefix.Match(page.KeyMarker, &match) {
			// FIXME: NO idea what S3 would do here.
			return result, gofakes3.ErrInternal
		}
		iter.Seek(page.KeyMarker)
	}

	var truncated = false
	var first = true
	var cnt int64 = 0

	// FIXME: The S3 docs have this to say on the topic of result ordering:
	//   "The following request returns objects in the order they were stored,
	//   returning the most recently stored object first starting with the value
	//   for key-marker."
	//
	// OK so this method....
	// - Returns objects in the order they were stored
	// - Returning the most recently stored object first
	//
	// This makes no sense at all!

	for iter.Next() {
		object := iter.Value().(*bucketObject)

		if !prefix.Match(object.name, &match) {
			continue
		}

		if match.CommonPrefix {
			result.AddPrefix(match.MatchedPart)
			continue
		}

		versions := iter.Value().(*bucketObject).Iterator()
		if first {
			if page.VersionIDMarker != "" {
				if !versions.Seek(page.VersionIDMarker) {
					// FIXME: log
					return result, gofakes3.ErrInternal
				}
			}
			first = false
		}

		for versions.Next() {
			version := versions.Value()

			if version.deleteMarker {
				marker := &gofakes3.DeleteMarker{
					Key:          version.name,
					IsLatest:     version == object.data,
					LastModified: gofakes3.NewContentTime(version.lastModified),
				}
				if bucket.versioning != gofakes3.VersioningNone { // S300005
					marker.VersionID = version.versionID
				}
				result.Versions = append(result.Versions, marker)

			} else {
				resultVer := &gofakes3.Version{
					Key:          version.name,
					IsLatest:     version == object.data,
					LastModified: gofakes3.NewContentTime(version.lastModified),
					Size:         int64(len(version.body)),
					ETag:         version.etag,
				}
				if bucket.versioning != gofakes3.VersioningNone { // S300005
					resultVer.VersionID = version.versionID
				}
				result.Versions = append(result.Versions, resultVer)
			}

			cnt++
			if page.MaxKeys > 0 && cnt >= page.MaxKeys {
				truncated = versions.Next()
				goto done
			}
		}
	}

done:
	result.IsTruncated = truncated || iter.Next()

	return result, nil
}

// nextVersion assumes the backend's lock is acquired
func (db *Backend) nextVersion() gofakes3.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
}
