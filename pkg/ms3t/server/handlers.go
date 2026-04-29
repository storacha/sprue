package server

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/storacha/sprue/pkg/ms3t/bucket"
)

const httpTimeFormat = http.TimeFormat // RFC1123 GMT

// --- Buckets ---

func (h *Handler) listBuckets(w http.ResponseWriter, r *http.Request) {
	states, err := h.svc.ListBuckets(reqCtx(r))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	resp := ListAllMyBucketsResult{
		Xmlns: s3Namespace,
		Owner: bucketsOwner{ID: "ms3t", DisplayName: "ms3t"},
	}
	for _, st := range states {
		resp.Buckets.Bucket = append(resp.Buckets.Bucket, bucketEntry{
			Name:         st.Name,
			CreationDate: time.Unix(st.CreatedAt, 0).UTC().Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, resp)
}

func (h *Handler) createBucket(w http.ResponseWriter, r *http.Request, name string) {
	err := h.svc.CreateBucket(reqCtx(r), name)
	if err != nil && !errors.Is(err, bucket.ErrBucketExists) {
		writeServiceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/"+name)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteBucket(w http.ResponseWriter, r *http.Request, name string) {
	if err := h.svc.DeleteBucket(reqCtx(r), name); err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) headBucket(w http.ResponseWriter, r *http.Request, name string) {
	if _, err := h.svc.List(reqCtx(r), name, bucket.ListOptions{MaxKeys: 1}); err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// --- Objects ---

func (h *Handler) putObject(w http.ResponseWriter, r *http.Request, name, key string) {
	defer r.Body.Close()

	// AWS SDKs default to chunked aws-chunked encoding, which we do NOT
	// decode here. Clients must disable streaming/chunked uploads or upload
	// small bodies in a single PUT.
	if v := r.Header.Get("x-amz-content-sha256"); v == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" || v == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" {
		writeError(w, http.StatusNotImplemented, "NotImplemented",
			"chunked aws-chunked uploads are not yet supported; configure the client to send unsigned/non-chunked payloads",
			r.URL.Path)
		return
	}

	mf, err := h.svc.PutObject(reqCtx(r), name, key, r.Body, r.Header.Get("Content-Type"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.Header().Set("ETag", etag(mf))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request, name, key string) {
	rng, rangeErr := parseRange(r.Header.Get("Range"))
	if rangeErr != nil {
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
			"invalid Range header", r.URL.Path)
		return
	}

	body, mf, err := h.svc.GetObject(reqCtx(r), name, key, rng)
	if err != nil {
		if errors.Is(err, bucket.ErrInvalidRange) {
			// We have the manifest; advertise the actual size for clients.
			if mf != nil {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", mf.Body.Size))
			}
			writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
				"requested range not satisfiable", r.URL.Path)
			return
		}
		writeServiceError(w, r, err)
		return
	}
	defer body.Close()

	if rng != nil {
		writeRangeHeaders(w, mf, rng)
		w.WriteHeader(http.StatusPartialContent)
	} else {
		writeObjectHeaders(w, mf)
		w.WriteHeader(http.StatusOK)
	}
	if _, err := io.Copy(w, body); err != nil {
		h.log.Warn("getobject body copy", "err", err, "key", key)
	}
}

func (h *Handler) headObject(w http.ResponseWriter, r *http.Request, name, key string) {
	mf, err := h.svc.HeadObject(reqCtx(r), name, key)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeObjectHeaders(w, mf)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteObject(w http.ResponseWriter, r *http.Request, name, key string) {
	if err := h.svc.DeleteObject(reqCtx(r), name, key); err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func etag(mf *bucket.ObjectManifest) string {
	return `"` + hex.EncodeToString(mf.Body.SHA256) + `"`
}

func writeObjectHeaders(w http.ResponseWriter, mf *bucket.ObjectManifest) {
	w.Header().Set("Content-Type", mf.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(mf.Body.Size, 10))
	w.Header().Set("ETag", etag(mf))
	w.Header().Set("Last-Modified", time.Unix(mf.Created, 0).UTC().Format(httpTimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
}

func writeRangeHeaders(w http.ResponseWriter, mf *bucket.ObjectManifest, rng *bucket.Range) {
	length := rng.End - rng.Start + 1
	w.Header().Set("Content-Type", mf.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("ETag", etag(mf))
	w.Header().Set("Last-Modified", time.Unix(mf.Created, 0).UTC().Format(httpTimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range",
		fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.End, mf.Body.Size))
}

// parseRange handles the single-range subset of RFC 7233 that S3 supports:
// "bytes=START-END", "bytes=START-", or "bytes=-SUFFIX". Multi-range
// requests are rejected. Empty header → no range.
//
// Returns (rng, nil) for a valid range, (nil, nil) when no Range header
// is present, (nil, err) on a malformed header. The "suffix" form
// (bytes=-N) cannot be resolved without the body size and is returned
// with Start=-1; the bucket service applies it after loading the manifest.
func parseRange(h string) (*bucket.Range, error) {
	if h == "" {
		return nil, nil
	}
	if !strings.HasPrefix(h, "bytes=") {
		return nil, errBadRange
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if strings.Contains(spec, ",") {
		return nil, errBadRange // multi-range not supported
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return nil, errBadRange
	}
	startStr := spec[:dash]
	endStr := spec[dash+1:]

	var start, end int64 = -1, -1
	var err error
	if startStr != "" {
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 {
			return nil, errBadRange
		}
	}
	if endStr != "" {
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < 0 {
			return nil, errBadRange
		}
	}

	switch {
	case startStr != "" && endStr != "":
		// "bytes=START-END"
		if end < start {
			return nil, errBadRange
		}
		return &bucket.Range{Start: start, End: end}, nil
	case startStr != "" && endStr == "":
		// "bytes=START-" — End resolved later by the service against Size.
		return &bucket.Range{Start: start, End: -1}, nil
	case startStr == "" && endStr != "":
		// "bytes=-SUFFIX" — last N bytes; encoded as Start=-1, End=N.
		return &bucket.Range{Start: -1, End: end}, nil
	default:
		return nil, errBadRange
	}
}

var errBadRange = errors.New("bad range header")

// --- Listing ---

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, name string) {
	q := r.URL.Query()

	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	startAfter := q.Get("start-after")
	token := q.Get("continuation-token")
	maxKeys := parseInt(q.Get("max-keys"), 1000)

	from := startAfter
	if token != "" {
		from = token
	}

	res, err := h.svc.List(reqCtx(r), name, bucket.ListOptions{
		Prefix:     prefix,
		Delimiter:  delimiter,
		StartAfter: from,
		MaxKeys:    maxKeys,
	})
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	resp := ListBucketResult{
		Xmlns:             s3Namespace,
		Name:              name,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		IsTruncated:       res.Truncated,
		KeyCount:          len(res.Objects) + len(res.CommonPrefixes),
		StartAfter:        startAfter,
		ContinuationToken: token,
	}
	if res.Truncated {
		resp.NextContinuationToken = res.NextToken
	}
	for _, mf := range res.Objects {
		resp.Contents = append(resp.Contents, objectEntry{
			Key:          mf.Key,
			LastModified: time.Unix(mf.Created, 0).UTC().Format(time.RFC3339),
			ETag:         etag(mf),
			Size:         mf.Body.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, cp := range res.CommonPrefixes {
		resp.CommonPrefixes = append(resp.CommonPrefixes, commonPrefix{Prefix: cp})
	}
	writeXML(w, http.StatusOK, resp)
}
