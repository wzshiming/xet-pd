package peer

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func newTestSpool(t *testing.T, size int64) *spool {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "spool-*.part")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return newSpool(f, size)
}

// readAsync reports how many bytes a Read returned, or fails the test if it returns too early.
func readAsync(t *testing.T, rd *spoolReader, buf []byte) <-chan int {
	t.Helper()
	got := make(chan int, 1)
	go func() {
		n, _ := rd.Read(buf)
		got <- n
	}()
	select {
	case n := <-got:
		t.Fatalf("Read returned %d bytes before they were published", n)
	case <-time.After(50 * time.Millisecond):
	}
	return got
}

func TestSpoolReaderBlocksAndWithholdsLastByte(t *testing.T) {
	s := newTestSpool(t, 8)
	rd := s.reader(context.Background())
	if n, err := rd.Seek(0, io.SeekEnd); err != nil || n != 8 {
		t.Fatalf("Seek end = %d, %v, want 8 without waiting", n, err)
	}
	if _, err := rd.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("negative offset accepted")
	}
	if _, err := rd.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if n, err := rd.Read(buf); n != 4 || err != nil || string(buf[:4]) != "abcd" {
		t.Fatalf("Read = %d %q %v, want abcd", n, buf[:n], err)
	}
	got := readAsync(t, rd, buf)
	if _, err := s.Write([]byte("efgh")); err != nil {
		t.Fatal(err)
	}
	if n := <-got; n != 3 || string(buf[:3]) != "efg" {
		t.Fatalf("read %d %q before validation, want efg with the last byte withheld", n, buf[:n])
	}
	got = readAsync(t, rd, buf)
	s.finish(nil)
	if n := <-got; n != 1 || buf[0] != 'h' {
		t.Fatalf("final read = %d %q", n, buf[:n])
	}
	if n, err := rd.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("read past end = %d, %v", n, err)
	}
}

func TestSpoolReaderCancelAndFailure(t *testing.T) {
	s := newTestSpool(t, 4)
	if _, err := s.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rd := s.reader(ctx)
	if _, err := rd.Seek(2, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() {
		_, err := rd.Read(make([]byte, 1))
		errc <- err
	}()
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = %v", err)
	}
	rd2 := s.reader(context.Background())
	if _, err := rd2.Seek(2, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, err := rd2.Read(make([]byte, 1))
		errc <- err
	}()
	boom := errors.New("boom")
	s.finish(boom)
	if err := <-errc; !errors.Is(err, boom) {
		t.Fatalf("read after failure = %v, want boom", err)
	}
	if !errors.Is(rd2.failed, boom) {
		t.Fatalf("reader did not record the failure: %v", rd2.failed)
	}
	u := newTestSpool(t, -1)
	if _, err := u.reader(context.Background()).Seek(0, io.SeekEnd); err == nil {
		t.Fatal("Seek end succeeded with unknown size")
	}
}

func TestSpoolReaderCloseInterruptsBlockedReadAndDetaches(t *testing.T) {
	s := newTestSpool(t, 4)
	if _, err := s.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	rd := s.reader(context.Background())
	if _, err := rd.Seek(2, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	got := readAsync(t, rd, buf)
	if err := rd.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close = %v, want the interrupted read reported as canceled", err)
	}
	if n := <-got; n != 0 {
		t.Fatalf("interrupted read returned %d bytes", n)
	}
	// The handler releases the file right after Close; nothing may reach it any more.
	if err := s.f.Close(); err != nil {
		t.Fatal(err)
	}
	s.finish(nil)
	if _, err := rd.Seek(0, io.SeekStart); !errors.Is(err, errReaderClosed) {
		t.Fatalf("Seek after Close = %v", err)
	}
	if n, err := rd.Read(buf); n != 0 || !errors.Is(err, errReaderClosed) {
		t.Fatalf("Read after Close = %d, %v", n, err)
	}
}
