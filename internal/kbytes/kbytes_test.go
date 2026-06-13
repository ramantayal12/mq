package kbytes

import "testing"

func TestPrimitiveRoundTrip(t *testing.T) {
	w := NewWriter()
	w.Int8(-5)
	w.Int16(-300)
	w.Int32(-70000)
	w.Int64(1 << 40)
	w.String("hello")
	w.NullableString(nil)
	s := "world"
	w.NullableString(&s)
	w.Bytes([]byte{1, 2, 3})
	w.Bytes(nil)

	r := NewReader(w.Finish())
	if got := r.Int8(); got != -5 {
		t.Fatalf("Int8=%d", got)
	}
	if got := r.Int16(); got != -300 {
		t.Fatalf("Int16=%d", got)
	}
	if got := r.Int32(); got != -70000 {
		t.Fatalf("Int32=%d", got)
	}
	if got := r.Int64(); got != 1<<40 {
		t.Fatalf("Int64=%d", got)
	}
	if got := r.String(); got != "hello" {
		t.Fatalf("String=%q", got)
	}
	if got := r.NullableString(); got != nil {
		t.Fatalf("NullableString want nil got %v", got)
	}
	if got := r.NullableString(); got == nil || *got != "world" {
		t.Fatalf("NullableString=%v", got)
	}
	if got := r.Bytes(); len(got) != 3 || got[0] != 1 {
		t.Fatalf("Bytes=%v", got)
	}
	if got := r.Bytes(); got != nil {
		t.Fatalf("Bytes want nil got %v", got)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestShortReadStickyError(t *testing.T) {
	r := NewReader([]byte{0, 1})
	r.Int32() // not enough bytes
	if r.Err() == nil {
		t.Fatal("expected short-read error")
	}
	// subsequent reads stay zero, no panic
	if r.Int64() != 0 {
		t.Fatal("expected zero after error")
	}
}

func TestVarintZigZag(t *testing.T) {
	// 1 -> uvarint 2; -1 -> uvarint 1
	r := NewReader([]byte{0x02})
	if got := r.Varint(); got != 1 {
		t.Fatalf("varint 0x02 => %d, want 1", got)
	}
	r = NewReader([]byte{0x01})
	if got := r.Varint(); got != -1 {
		t.Fatalf("varint 0x01 => %d, want -1", got)
	}
}
