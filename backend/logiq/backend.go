package logiq

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	gofakes3 "github.com/logiqai/s32http"
	"github.com/logiqai/s32http/internal/goskipiter"
	"github.com/sirupsen/logrus"
)

var (
	emptyPrefix       = &gofakes3.Prefix{}
	emptyVersionsPage = &gofakes3.ListBucketVersionsPage{}
	// Create a new instance of the logger. You can have any number of instances.
	log = logrus.New()
)

type Backend struct {
	buckets                   map[string]*bucket
	timeSource                gofakes3.TimeSource
	versionGenerator          *versionGenerator
	versionSeed               int64
	versionSeedSet            bool
	versionScratch            []byte
	lock                      sync.RWMutex
	logiqHost                 string
	logiqPort                 string
	maxWorkers                int
	maxBatch                  int
	maxObjectPropagationCount int
	proxySlots                chan struct{}
}

var _ gofakes3.Backend = &Backend{}
var _ gofakes3.VersionedBackend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource gofakes3.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func WithMaxObjectPropagationCount(maxObjectPropagationCount int) Option {
	log.Infoln("Setting max object propagation count to", maxObjectPropagationCount)
	if maxObjectPropagationCount <= 0 {
		maxObjectPropagationCount = 1
	}
	return func(b *Backend) { b.maxObjectPropagationCount = maxObjectPropagationCount }

}

func WithMaxWorkers(maxWorkers int) Option {
	log.Infoln("Setting max workers to", maxWorkers)
	return func(b *Backend) { b.maxWorkers = maxWorkers }
}

func WithMaxBatch(maxBatch int) Option {
	log.Infoln("Setting max batch to", maxBatch)
	return func(b *Backend) { b.maxBatch = maxBatch }
}

func WithLogiqHost(host string) Option {
	log.Infoln("Setting logiq host to", host)
	return func(b *Backend) { b.logiqHost = host }
}

func WithLogiqPort(port string) Option {
	log.Infoln("Setting logiq port to", port)
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
		buckets: make(map[string]*bucket),
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
	b.proxySlots = make(chan struct{}, b.maxObjectPropagationCount)
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

func (db *Backend) SendToLogiq(bts []byte) error {
	db.waitForProxySlot()
	defer db.emptyProxySlot()
	m := []map[string]interface{}{}
	err := json.Unmarshal(bts, &m)
	if err != nil {
		return err
	}
	ch := make(chan bool, db.maxWorkers)
	wg := sync.WaitGroup{}
	retErrors := sync.Map{}
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
		wg.Add(1)
		go func(wgPtr *sync.WaitGroup, index int, batchBytes []byte, doneCh chan bool) {
			defer func() {
				<-ch
				wgPtr.Done()
			}()
			sendErr := db.SendToLogiqSingleBatch(batchBytes)
			if sendErr != nil {
				retErrors.Store(index, sendErr)
			}
		}(&wg, i, b, ch)
	}
	wg.Wait()
	allerrors := []string{}
	retErrors.Range(func(key, value interface{}) bool {
		allerrors = append(allerrors, value.(error).Error())
		return true
	})
	if len(allerrors) > 0 {
		return errors.New(strings.Join(allerrors, ","))
	} else {
		return nil
	}
}

func (db *Backend) emptyProxySlot() {
	if len(db.proxySlots) > 0 {
		<-db.proxySlots
	}
}

func (db *Backend) waitForProxySlot() {
	db.proxySlots <- struct{}{}
}

func (db *Backend) SendToLogiqSingleBatch(bts []byte) error {
	logiqClient := &http.Client{}
	logiqURL := fmt.Sprintf("http://%s:%s/v1/json_batch", db.logiqHost, db.logiqPort)
	logiqClient.Timeout = time.Second * 60
	token, exists := os.LookupEnv("x_api_key")

	req, reqErr := http.NewRequest("POST", logiqURL, bytes.NewBuffer(bts))
	if reqErr != nil {
		log.Errorf("Error creating request: %v", reqErr)
		return reqErr
	}
	if exists {
		req.Header.Add("Content-type", "application/json")
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	} else {
		req.Header.Add("Content-type", "application/json")
	}
	resp, respErr := logiqClient.Do(req)
	if respErr != nil {
		log.Errorf("Error sending to logiq: %v", respErr)
		return respErr
	} else {
		body, bodyErr := ioutil.ReadAll(resp.Body)
		if bodyErr != nil {
			log.Errorf("Error reading response from logiq: %v", bodyErr)
			return bodyErr
		} else {
			log.Debugf("Response from logiq: %v", string(body))
		}
	}
	return nil
}

func (db *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (result gofakes3.PutObjectResult, err error) {
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

	db.lock.RLock()
	bucket := db.buckets[bucketName]
	db.lock.RUnlock()
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	retval := db.SendToLogiq(bts)
	if retval != nil {
		log.Printf("Error sending to logiq: %v", retval)
		return result, retval
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
