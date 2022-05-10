// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yixinin/gokv/impls/wal/walpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

// Repair tries to repair ErrUnexpectedEOF in the
// last wal file by truncating.
func Repair(lg *logrus.Logger, dirpath string) bool {
	if lg == nil {
		lg = logrus.StandardLogger()
	}
	f, err := openLast(lg, dirpath)
	if err != nil {
		return false
	}
	defer f.Close()

	lg.Info("repairing", logrus.WithField("path", f.Name()))

	rec := &walpb.Record{}
	decoder := newDecoder(f)
	for {
		lastOffset := decoder.lastOffset()
		err := decoder.decode(rec)
		switch err {
		case nil:
			// update crc of the decoder when necessary
			switch rec.Type {
			case crcType:
				crc := decoder.crc.Sum32()
				// current crc of decoder must match the crc of the record.
				// do no need to match 0 crc, since the decoder is a new one at this case.
				if crc != 0 && rec.Validate(crc) != nil {
					return false
				}
				decoder.updateCRC(rec.Crc)
			}
			continue

		case io.EOF:
			lg.Info("repaired", logrus.WithField("path", f.Name()), logrus.WithError(io.EOF))
			return true

		case io.ErrUnexpectedEOF:
			brokenName := f.Name() + ".broken"
			bf, bferr := os.Create(brokenName)
			if bferr != nil {
				lg.Warn("failed to create backup file", logrus.WithField("path", brokenName), logrus.WithError(bferr))
				return false
			}
			defer bf.Close()

			if _, err = f.Seek(0, io.SeekStart); err != nil {
				lg.Warn("failed to read file", logrus.WithField("path", f.Name()), logrus.WithError(err))
				return false
			}

			if _, err = io.Copy(bf, f); err != nil {
				lg.Warn("failed to copy", logrus.WithField("from", f.Name()), logrus.WithField("to", brokenName), logrus.WithError(err))
				return false
			}

			if err = f.Truncate(lastOffset); err != nil {
				lg.Warn("failed to truncate", logrus.WithField("path", f.Name()), logrus.WithError(err))
				return false
			}

			start := time.Now()
			if err = fileutil.Fsync(f.File); err != nil {
				lg.Warn("failed to fsync", logrus.WithField("path", f.Name()), logrus.WithError(err))
				return false
			}
			walFsyncSec.Observe(time.Since(start).Seconds())

			lg.Info("repaired", logrus.WithField("path", f.Name()), logrus.WithError(io.ErrUnexpectedEOF))
			return true

		default:
			lg.Warn("failed to repair", logrus.WithField("path", f.Name()), logrus.WithError(err))
			return false
		}
	}
}

// openLast opens the last wal file for read and write.
func openLast(lg *logrus.Logger, dirpath string) (*fileutil.LockedFile, error) {
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, err
	}
	last := filepath.Join(dirpath, names[len(names)-1])
	return fileutil.LockFile(last, os.O_RDWR, fileutil.PrivateFileMode)
}
