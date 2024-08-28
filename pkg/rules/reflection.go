package rules

import (
	"fmt"
	"reflect"
)

type (
	FuncMap    map[string]interface{}
	ValueFuncs map[string]reflect.Value
)

var (
	errorType = reflect.TypeOf((*error)(nil)).Elem()
)

func MakeValueFuncs(funcs FuncMap) ValueFuncs {
	out := make(ValueFuncs)
	for name, fn := range funcs {
		v := reflect.ValueOf(fn)
		if v.Kind() != reflect.Func {
			panic(name + " not a function")
		}
		if !goodFunc(v.Type()) {
			panic(fmt.Errorf("can't add function %q with %d results", name, v.Type().NumOut()))
		}
		out[name] = v
	}
	return out
}

func callFunc(funcs map[string]reflect.Value, name string, params ...interface{}) (interface{}, error) {
	fn, ok := funcs[name]
	if !ok {
		return nil, fmt.Errorf("function not found: %s", name)
	}

	fnType := fn.Type()
	fnNumIn := fnType.NumIn()
	paramsLen := len(params)

	in := make([]reflect.Value, fnNumIn)
	for i := 0; i < fnNumIn; i++ {
		if i < paramsLen {
			p := params[i]
			in[i] = reflect.ValueOf(p)
		} else {
			a := fnType.In(i)
			in[i] = reflect.Zero(a)
		}
	}

	ret, err := safeCall(fn, in)
	if err != nil {
		return nil, err
	}

	return ret.Interface(), nil
}

// safeCall runs fn.Call(args), and returns the resulting error if any
// if the call panics, the panic value is returned as an error
func safeCall(fn reflect.Value, args []reflect.Value) (val reflect.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	ret := fn.Call(args)

	// return (res, err)
	if len(ret) == 2 && !ret[1].IsNil() {
		return ret[0], ret[1].Interface().(error)
	}

	// return (err)
	if len(ret) == 1 && !ret[0].IsNil() {
		return ret[0], ret[0].Interface().(error)
	}

	return ret[0], nil
}

// goodFunc reports whether the function or method has the right result signature
// we allow functions with 1 result or 2 results where the second is an error
func goodFunc(typ reflect.Type) bool {
	switch {
	case typ.NumOut() == 1:
		return true
	case typ.NumOut() == 2 && typ.Out(1) == errorType:
		return true
	}
	return false
}
