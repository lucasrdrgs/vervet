module vervet

pub fn as_f64(arr []string) []f64 {
	mut final := []f64{}
	for x in arr {
		final << x.f64()
	}
	return final
}

pub fn as_int(arr []string) []int {
	mut final := []int{}
	for x in arr {
		final << x.int()
	}
	return final
}
