package transcode

import (
	"context"
	"io"
)

type SeekTranscoder struct {
	ffmpeg *FFmpegTranscoder
}

var _ Transcoder = (*SeekTranscoder)(nil)

func NewSeekTranscoder() SeekTranscoder {
	return SeekTranscoder{ffmpeg: NewFFmpegTranscoder()}
}

func (s *SeekTranscoder) Transcode(ctx context.Context, profile Profile, in string, out io.Writer) error {
	return s.ffmpeg.Transcode(ctx, seek, in, out)
}
