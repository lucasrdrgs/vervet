import vervet

fn main() {
	mut df := vervet.read_csv(
		path: 'sample1.csv',
		sep: ','
	)

	println('Dataset 1, single chunk:')
	println(df.tabulate())
	println('')
	df.close() // It's nice to close the dataframe after using it.

	df = vervet.read_csv(
		path: 'sample2.csv',
		sep: ',',
		wrapper: `"`,
		chunk_size: 3
	)

	println('Dataset 2, chunk size of 3, first chunk:')
	println(df.tabulate())
	println('')

	println('Dataset 2, chunk size of 3, second chunk:')
	df.next_chunk()
	println(df.tabulate())
	println('')
	df.close()

	df = vervet.read_csv(
		path: 'sample3.csv',
		sep: ',',
		columns: ['colA', 'colB']
	)

	println('Dataset 3, single chunk, no columns in CSV, columns specified in read_csv:')
	println(df.tabulate())
	df.close()
}
