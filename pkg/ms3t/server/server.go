// Package server exposes the bucket service over an S3-compatible HTTP API.
// Path-style addressing only (clients must set forcePathStyle=true).
//
// Auth is intentionally not validated: the Authorization header is read and
// logged so the request can be traced, but its contents are ignored. Real
// auth is a future middleware; this matches the localstack/MinIO-test style
// of giving the SDK a credential to sign with.
package server

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/storacha/sprue/pkg/ms3t/bucket"
	"github.com/storacha/sprue/pkg/ms3t/registry"
)

// Handler implements http.Handler over a *bucket.Service.
type Handler struct {
	svc *bucket.Service
	log *slog.Logger
}

// New returns an http.Handler for the bucket service.
func New(svc *bucket.Service, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{svc: svc, log: log}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Trim leading slash, split into at most 2 components.
	path := strings.TrimPrefix(r.URL.Path, "/")
	var bucketName, key string
	if path != "" {
		if i := strings.Index(path, "/"); i >= 0 {
			bucketName, key = path[:i], path[i+1:]
		} else {
			bucketName = path
		}
	}

	h.log.Debug("s3 request",
		"method", r.Method,
		"bucket", bucketName,
		"key", key,
		"query", r.URL.RawQuery,
		"auth", r.Header.Get("Authorization") != "")

	switch {
	case bucketName == "" && r.Method == http.MethodGet:
		h.listBuckets(w, r)
	case key == "" && r.Method == http.MethodPut:
		h.createBucket(w, r, bucketName)
	case key == "" && r.Method == http.MethodDelete:
		h.deleteBucket(w, r, bucketName)
	case key == "" && r.Method == http.MethodGet:
		h.listObjects(w, r, bucketName)
	case key == "" && r.Method == http.MethodHead:
		h.headBucket(w, r, bucketName)
	case key != "" && r.Method == http.MethodPut:
		h.putObject(w, r, bucketName, key)
	case key != "" && r.Method == http.MethodGet:
		h.getObject(w, r, bucketName, key)
	case key != "" && r.Method == http.MethodHead:
		h.headObject(w, r, bucketName, key)
	case key != "" && r.Method == http.MethodDelete:
		h.deleteObject(w, r, bucketName, key)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed",
			fmt.Sprintf("method %s not allowed for this resource", r.Method), r.URL.Path)
	}
}

// === Helpers ===

func writeXML(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, msg, resource string) {
	writeXML(w, status, ErrorResponse{
		Code: code, Message: msg, Resource: resource,
	})
}

func mapServiceError(err error) (status int, code, msg string) {
	switch {
	case errors.Is(err, bucket.ErrBucketNotFound):
		return http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist"
	case errors.Is(err, bucket.ErrObjectNotFound):
		return http.StatusNotFound, "NoSuchKey", "The specified key does not exist"
	case errors.Is(err, bucket.ErrBucketExists), errors.Is(err, registry.ErrExists):
		return http.StatusConflict, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded"
	case errors.Is(err, bucket.ErrInvalidBucket):
		return http.StatusBadRequest, "InvalidBucketName", "The specified bucket is not valid"
	case errors.Is(err, bucket.ErrInvalidKey):
		return http.StatusBadRequest, "InvalidArgument", "Object key is invalid"
	case errors.Is(err, bucket.ErrBucketNotEmpty):
		return http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty"
	default:
		return http.StatusInternalServerError, "InternalError", err.Error()
	}
}

func writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, msg := mapServiceError(err)
	writeError(w, status, code, msg, r.URL.Path)
}

func parseInt(s string, dflt int) int {
	if s == "" {
		return dflt
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return dflt
	}
	return n
}

func reqCtx(r *http.Request) context.Context { return r.Context() }
