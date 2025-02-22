package transcode

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const perm = 0o644

type CachingTranscoder struct {
	cachePath      string
	transcoder     Transcoder
	seekTranscoder SeekTranscoder
	limitMB        int
	locks          keyedMutex
	cleanLock      sync.RWMutex
}

var _ Transcoder = (*CachingTranscoder)(nil)

func NewCachingTranscoder(t Transcoder, cachePath string, limitMB int) *CachingTranscoder {
	return &CachingTranscoder{transcoder: t, cachePath: cachePath, limitMB: limitMB, seekTranscoder: NewSeekTranscoder()}
}

func (t *CachingTranscoder) Transcode(ctx context.Context, profile Profile, in string, out io.Writer) error {
	t.cleanLock.RLock()
	defer t.cleanLock.RUnlock()

	if err := os.MkdirAll(t.cachePath, perm^0o111); err != nil {
		return fmt.Errorf("make cache path: %w", err)
	}

	name, args, err := parseProfile(profile, in)
	if err != nil {
		return fmt.Errorf("split command: %w", err)
	}

	key := cacheKey(name, args)
	unlock := t.locks.Lock(key)
	defer unlock()

	path := filepath.Join(t.cachePath, key)
	cf, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		return fmt.Errorf("open cache file: %w", err)
	}
	defer cf.Close()

	i, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat cache file: %w", err)
	}

	if i.Size() > 0 {
		_ = os.Chtimes(path, time.Now(), time.Now()) // Touch for LRU cache purposes
		if profile.Seek() > 0 {
			// it's already been transcoded, so just seek into it
			return t.seekTranscoder.Transcode(ctx, profile, in, out)
		} else {
			// if the seek is zero, just copy the whole file
			n, err := io.Copy(out, cf)
			if n != i.Size() {
				return fmt.Errorf("should have wrote %d bytes, but actually wrote %d", i.Size(), n)
			}
			return err
		}
	}

	zeroProfile := profile
	var fileOut io.Writer = cf
	if profile.Seek() == 0 {
		// If seek is zero, transcode to disc and copy out to socket at same time
		fileOut = io.MultiWriter(out, cf)
	} else {
		// If its non-zero, we force the profile to seek from 0 for
		// caching purposes...
		zeroProfile = WithSeek(profile, 0)
	}

	if err := t.transcoder.Transcode(ctx, zeroProfile, in, fileOut); err != nil {
		os.Remove(path)
		return fmt.Errorf("internal transcode: %w", err)
	}

	// ...and then we finally send the seeked data
	if profile.Seek() != 0 {
		return t.seekTranscoder.Transcode(ctx, profile, in, out)
	} // (if the seek is zero, the MultiWriter will have already written the data)

	return nil
}

func (t *CachingTranscoder) CacheEject() error {
	t.cleanLock.Lock()
	defer t.cleanLock.Unlock()

	// Delete LRU cache files that exceed size limit. Use last modified time.
	type file struct {
		path string
		info os.FileInfo
	}

	var files []file
	var total int64 = 0

	err := filepath.WalkDir(t.cachePath, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !de.IsDir() {
			info, err := de.Info()
			if err != nil {
				return fmt.Errorf("walk cache path for eject: %w", err)
			}
			files = append(files, file{path, info})
			total += info.Size()
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("walk cache path for eject: %w", err)
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].info.ModTime().Before(files[j].info.ModTime())
	})

	for total > int64(t.limitMB)*1024*1024 {
		curFile := files[0]
		files = files[1:]
		total -= curFile.info.Size()
		err = os.Remove(curFile.path)
		if err != nil {
			return fmt.Errorf("remove cache file: %w", err)
		}
	}

	return nil
}

func cacheKey(cmd string, args []string) string {
	// the cache is invalid whenever transcode command (which includes the
	// absolute filepath, bit rate args, replay gain args, etc.) changes
	sum := md5.New()
	_, _ = io.WriteString(sum, cmd)
	for _, arg := range args {
		_, _ = io.WriteString(sum, arg)
	}
	return fmt.Sprintf("%x", sum.Sum(nil))
}

type keyedMutex struct {
	sync.Map
}

func (km *keyedMutex) Lock(key string) func() {
	value, _ := km.LoadOrStore(key, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	// TODO: remove key entry from map to save some space?
	return mu.Unlock
}
