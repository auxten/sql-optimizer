package utils

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSortDeDup(t *testing.T) {
	Convey("sort and deduplicate", t, func() {
		l := []string{"c", "a", "b", "c", "e", "d"}
		sl := SortDeDup(l)
		So(sl, ShouldResemble, []string{"a", "b", "c", "d", "e"})
	})
	Convey("sort and deduplicate", t, func() {
		l := []string{"mj.mc_trscode", "mj.rowid", "mj.tans_amt", "mj.trans_bran_code", "mj.trans_date", "mj.trans_flag", "trans_date", "mj.trans_flag", "trans_date"}
		sl := SortDeDup(l)
		So(sl, ShouldResemble, []string{"mj.mc_trscode", "mj.rowid", "mj.tans_amt", "mj.trans_bran_code", "mj.trans_date", "mj.trans_flag", "trans_date"})
	})
	Convey("sort and deduplicate 1", t, func() {
		l := []string{"c"}
		sl := SortDeDup(l)
		So(sl, ShouldResemble, []string{"c"})
	})
	Convey("sort and deduplicate 0", t, func() {
		l := make([]string, 0)
		sl := SortDeDup(l)
		So(sl, ShouldResemble, []string{})
	})
	Convey("sort and deduplicate nil", t, func() {
		var l []string
		sl := SortDeDup(l)
		So(sl, ShouldResemble, []string(nil))
	})
}
