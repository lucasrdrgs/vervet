module vervet

import os
import serkonda7.termtable as tt

pub struct Dataframe {
mut:
	n_chunks	int
	rows_chk	int				// Rows per chunk
	cur_chk		int = -1
	cur_byte	u64
	file		os.File
	file_size	u64
	options		CsvOptions
pub:
	num_rows	int
	columns		[]string
pub mut:
	data		[][]string
}

pub struct CsvOptions {
pub:
	path		string
	sep			string = ','
	fill_na		string
	columns		[]string
	chunk_size	int
	wrapper		rune = `"`		// "This string is wrapped by double quotes"
}

pub fn (mut df Dataframe) close() {
	df.file.close()
}

pub fn (df Dataframe) tabulate() string {
	mut data := [][]string{}
	data << df.columns
	data << df.data

	return (tt.Table{
		data: data,
		style: .pretty,
		header_style: .bold,
		align: .center
	}).str()
}

pub fn (df Dataframe) str() string {
	return df.tabulate()
}

pub fn (mut df Dataframe) next_chunk() {
	if df.cur_chk + 1 == df.n_chunks {
		panic('Maximum number of chunks reached.')
	}
	df.cur_chk++
	df.data = [][]string{}
	mut current_line := ''
	for {
		if df.cur_byte >= df.file_size || df.data.len >= df.rows_chk {
			break
		}
		cur_byte := df.file.read_bytes_at(1, df.cur_byte)[0]
		if cur_byte == byte(0xA) {
			parsed := parse_row(current_line, df.options.sep, df.options.fill_na, df.options.wrapper)
			df.data << parsed
			df.cur_byte++
			current_line = ''
			continue
		}
		else if cur_byte == byte(0xD) {
			df.cur_byte++
			continue
		}
		current_line += [cur_byte].bytestr()
		df.cur_byte++
	}
}

pub fn (mut df Dataframe) get_row_at(index int) []string {
	mut tmp_index := index
	mut has_changed_chk := false
	for tmp_index > df.data.len - 1 {
		tmp_index -= df.rows_chk
		df.next_chunk()
		has_changed_chk = true
	}
	to_ret := df.data[tmp_index]
	if has_changed_chk {
		df.reset()
	}
	return to_ret
}

pub fn (mut df Dataframe) reset() {
	df.cur_byte = 0
	df.cur_chk = -1
	df.next_chunk()
}

pub fn (df Dataframe) get_col(col string) []string {
	if col !in df.columns {
		panic('Column "$col" does not exist in the dataframe.')
	}
	return df.get_col_at(df.columns.index(col))
}

pub fn (df Dataframe) get_col_at(index int) []string {
	mut to_return := []string{}
	for row in df.data {
		to_return << row[index]
	}
	return to_return
}

pub fn read_csv(o CsvOptions) Dataframe {
	if !os.is_file(o.path) {
		panic('File "$o.path" does not exist.')
	}
	mut fp := os.open(o.path) or {
		panic('File "$o.path" could not be read. Do you have the appropriate permissions?')
	}

	mut file_size := os.file_size(o.path)
	mut rem_file_size := file_size
	if file_size == 0 {
		panic('File "$o.path" seems to be empty.')
	}

	mut chk_sz := o.chunk_size

	mut k := u64(0)
	mut n_rows := 0
	mut read_chunk_size := 1024
	for rem_file_size > 0 {
		mut to_read := read_chunk_size
		if to_read > rem_file_size {
			to_read = int(rem_file_size)
		}

		r := fp.read_bytes_at(to_read, k * u64(read_chunk_size))
		for b in r {
			if b == byte(0xA) { // newline \n
				n_rows++
			}
		}

		rem_file_size -= u64(to_read)
		k++
	}
	if chk_sz <= 0 {
		chk_sz = n_rows
	}
	mut n_chunks := int(n_rows / chk_sz)
	if int((f64(n_rows) / f64(chk_sz)) * 10.0) % 10 != 0 {
		n_chunks++
	}

	mut columns := []string{}
	mut current_line := ''
	k = 0 // starting point for reading
	if o.columns.len == 0 {
		for {
			if k >= file_size {
				break
			}
			cur_byte := fp.read_bytes_at(1, k)[0]
			if cur_byte == byte(0xA) {
				columns = parse_row(current_line, o.sep, o.fill_na, o.wrapper)
				k++
				break
			}
			else if cur_byte == byte(0xD) {
				k++
				continue
			}
			current_line += [cur_byte].bytestr()
			k++
		}
	}
	else {
		columns = o.columns
	}

	mut df := Dataframe{
		n_chunks: n_chunks,
		rows_chk: chk_sz,
		num_rows: n_rows - 1,
		cur_chk: -1,
		cur_byte: k,
		file: fp,
		file_size: file_size,
		columns: columns,
		data: [][]string{},
		options: o
	}

	df.next_chunk()

	// fp is not closed. This is intentional to allow the user to
	// read chunks. The user is expected to run df.close() once
	// they're done with dataframe operations.

	return df
}

// please work
pub fn (mut df Dataframe) to_csv(o CsvOptions) {
	// Write first chunk
	mut fp := os.open_file(o.path, 'w') or {
		panic('File "$o.path" could not be opened to write. Do you have the appropriate permissions?')
	}
	mut s_write := df.columns.join(o.sep) + '\n'
	for row in df.data {
		s_write += row.join(o.sep) + '\n'
	}
	fp.write_string(s_write) or {
		panic('File "$o.path" could not be written to. Do you have the appropriate permissions?')
	}
	fp.close()

	if df.n_chunks > 1 {
		fp = os.open_file(o.path, 'a') or {
			panic('File "$o.path" could not be opened to append. Do you have the appropriate permissions?')
		}
		for df.cur_chk < df.n_chunks {
			s_write = ''
			df.next_chunk()
			for row in df.data {
				s_write += row.join(o.sep)
			}
			fp.write_string(s_write) or {
				panic('File "$o.path" could not be written to. Do you have the appropriate permissions?')
			}
		}
		df.reset()
		fp.close()
	}
}
