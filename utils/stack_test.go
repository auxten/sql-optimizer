package utils

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStack(t *testing.T) {
	Convey("stack", t, func() {
		s := NewStack(0)
		s.Push(1)
		So(s.Len(), ShouldEqual, 1)

		s2 := s.Copy(0, s.Len())
		item, ok := s2.Get(0)
		So(ok, ShouldBeTrue)
		So(item, ShouldEqual, 1)
		So(s.Len(), ShouldEqual, 1)

		item, ok = s.Get(0)
		So(ok, ShouldBeTrue)
		So(item, ShouldEqual, 1)
		So(s.Len(), ShouldEqual, 1)

		item, ok = s.Pop()
		So(ok, ShouldBeTrue)
		So(item, ShouldEqual, 1)
		item, ok = s.Pop()
		So(ok, ShouldBeFalse)
		So(item, ShouldBeNil)

		item, ok = s.Get(0)
		So(ok, ShouldBeFalse)
		So(item, ShouldBeNil)
	})
}
func TestStackLong(t *testing.T) {
	Convey("stack long", t, func() {
		const cnt = 1000
		s := NewStack(0)
		for i := 0; i < cnt; i++ {
			s.Push(i)
		}
		So(s.Len(), ShouldEqual, cnt)

		for i := 0; i < cnt; i++ {
			n, ok := s.Pop()
			So(ok, ShouldBeTrue)
			So(n, ShouldEqual, cnt-i-1)
		}
		So(s.Len(), ShouldEqual, 0)
	})
}
