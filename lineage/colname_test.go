package lineage

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestColumnName(t *testing.T) {
	Convey("ColumnName", t, func() {
		cn := ColumnName("")
		So(cn.ColName(), ShouldResemble, "")
		So(cn.TabName(), ShouldResemble, "")
		cn = ColumnName("t1.c1")
		So(cn.ColName(), ShouldResemble, "c1")
		So(cn.TabName(), ShouldResemble, "t1")
		cn = ColumnName("t1.")
		So(cn.ColName(), ShouldResemble, "")
		So(cn.TabName(), ShouldResemble, "t1")
		cn = ColumnName(".c1")
		So(cn.ColName(), ShouldResemble, "c1")
		So(cn.TabName(), ShouldResemble, "")
		cn = ColumnName("c1")
		So(cn.ColName(), ShouldResemble, "c1")
		So(cn.TabName(), ShouldResemble, "")
		cp := NewColumnName("t1", "c1")
		So(cp.ColName(), ShouldResemble, "c1")
		So(cp.TabName(), ShouldResemble, "t1")
	})
}
