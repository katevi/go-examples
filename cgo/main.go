package main

/*
#cgo CFLAGS: -I${SRCDIR}/ctestlib
#cgo LDFLAGS: -Wl,-rpath,${SRCDIR}/build
#cgo LDFLAGS: -L${SRCDIR}/build
#cgo LDFLAGS: -ltest

#include <test.h>
*/
import "C"
import "fmt"

func main() {
	sum := C.sum(3, 5)
	fmt.Println(sum)
}
