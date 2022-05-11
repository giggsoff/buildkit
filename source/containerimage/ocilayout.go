package containerimage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/moby/buildkit/session/filesync"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes"
	"github.com/moby/buildkit/session"
	sessioncontent "github.com/moby/buildkit/session/content"
	"github.com/moby/buildkit/source"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	ociImageIndexFile = "index.json"
)

// GetOCILayoutResolver gets a resolver to an OCI layout for a specified scope from the pool
func GetOCILayoutResolver(layoutPath string, sm *session.Manager, sessionID string, g session.Group) *OCILayoutResolver {
	fileStore, err := local.NewStore(layoutPath)
	if err != nil {
		return nil
	}
	r := &OCILayoutResolver{
		path:      layoutPath,
		sm:        sm,
		sessionID: sessionID,
		g:         g,
		store:     fileStore,
	}
	return r
}

type OCILayoutResolver struct {
	remotes.Resolver
	path      string
	sm        *session.Manager
	sessionID string
	g         session.Group
	is        images.Store
	mode      source.ResolveMode
	store     content.Store
}

// WithImageStore returns new resolver that can also resolve from local images store
func (r *OCILayoutResolver) WithImageStore(is images.Store, mode source.ResolveMode) *OCILayoutResolver {
	r2 := *r
	r2.is = is
	r2.mode = mode
	return &r2
}

// Fetcher returns a new fetcher for the provided reference.
func (r *OCILayoutResolver) Fetcher(ctx context.Context, ref string) (remotes.Fetcher, error) {
	return r, nil
}

// Fetch get an io.ReadCloser for the specific content
func (r *OCILayoutResolver) Fetch(ctx context.Context, desc ocispecs.Descriptor) (io.ReadCloser, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	sessionID := r.sessionID

	caller, err := r.sm.Get(timeoutCtx, sessionID, false)
	if err != nil {
		return r.fetchWithAnySession(ctx, desc)
	}

	return r.fetchWithSession(ctx, desc, caller)
}

func (r *OCILayoutResolver) fetchWithAnySession(ctx context.Context, desc ocispecs.Descriptor) (io.ReadCloser, error) {
	var rc io.ReadCloser
	err := r.sm.Any(ctx, r.g, func(ctx context.Context, _ string, caller session.Caller) error {
		readCloser, err := r.fetchWithSession(ctx, desc, caller)
		if err != nil {
			return err
		}
		rc = readCloser
		return nil
	})
	return rc, err
}

func (r *OCILayoutResolver) fetchWithSession(ctx context.Context, desc ocispecs.Descriptor, caller session.Caller) (io.ReadCloser, error) {
	store := sessioncontent.NewCallerStore(caller, "oci-layout:"+r.path)
	readerAt, err := store.ReaderAt(ctx, desc)
	if err != nil {
		return nil, err
	}
	// just wrap the ReaderAt with a Reader
	return ioutil.NopCloser(&readerAtWrapper{readerAt: readerAt}), nil
}

// Resolve attempts to resolve the reference into a name and descriptor
func (r *OCILayoutResolver) Resolve(ctx context.Context, ref string) (string, ocispecs.Descriptor, error) {
	sessionID := r.sessionID

	caller, err := r.sm.Get(ctx, sessionID, false)
	if err != nil {
		return ref, ocispecs.Descriptor{}, errors.New(fmt.Sprintf("cannot get caller %s", sessionID))
	}
	tmpDir, err := os.MkdirTemp("", "oci-layout:index")
	if err != nil {
		return ref, ocispecs.Descriptor{}, err
	}
	defer os.RemoveAll(tmpDir)
	o := filesync.FSSendRequestOpt{
		Name:            "oci-layout:" + r.path + "index",
		DestDir:         tmpDir,
		IncludePatterns: []string{"index.json"},
	}
	err = filesync.FSSync(ctx, caller, o)
	if err != nil {
		return ref, ocispecs.Descriptor{}, err
	}
	fReader, err := os.Open(filepath.Join(tmpDir, ociImageIndexFile))
	if err != nil {
		return ref, ocispecs.Descriptor{}, err
	}

	var mfst ocispecs.Index
	decoder := json.NewDecoder(fReader)
	err = decoder.Decode(&mfst)
	if err != nil {
		return ref, ocispecs.Descriptor{}, err
	}
	refParsed, err := reference.Parse(ref)
	if err != nil {
		return ref, ocispecs.Descriptor{}, err
	}
	for _, el := range mfst.Manifests {
		//FIXME add support for resolve by tag
		if el.Digest == refParsed.Digest() {
			return ref, el, nil
		}
	}
	return ref, ocispecs.Descriptor{}, errors.New(fmt.Sprintf("not found %s", ref))
}

func (r *OCILayoutResolver) Pusher(ctx context.Context, ref string) (remotes.Pusher, error) {
	return nil, errors.New("unsupported")
}

// readerAtWrapper wraps a ReaderAt to give a Reader
type readerAtWrapper struct {
	offset   int64
	readerAt io.ReaderAt
}

func (r *readerAtWrapper) Read(p []byte) (n int, err error) {
	n, err = r.readerAt.ReadAt(p, r.offset)
	r.offset += int64(n)
	return
}
