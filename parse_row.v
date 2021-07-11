module vervet

fn parse_row(row_o string, sep string, fill_na string, wrapper rune) []string {
	// A very stupid hack to escape the wrapper char: "Hello \"there\"!"
	mut substr := '@'
	for row_o.contains(substr) {
		substr += '@'
	}
	mut row := row_o.replace('\\' + wrapper.str(), substr)
	mut cells := []string{}
	if row.trim(' \t\n').len == 0 {
		return cells
	}
	sep_len := sep.len
	mut is_wrapped := false
	mut piece := ''
	mut i := 0
	for {
		if i >= row.len {
			if row[row.len - 1] != '\n'[0] {
				cells << piece
			}
			break
		}
		current := rune(row[i])
		if current == wrapper && !is_wrapped {
			is_wrapped = true
			i++
			continue
		}
		else if current == wrapper && is_wrapped {
			is_wrapped = false
			i++
			continue
		}
		if row[i..(i + sep_len)] == sep {
			if !is_wrapped {
				cells << piece
				piece = ''
			}
			else {
				piece += sep
			}
			i += sep_len
			continue
		}
		if row[i] == '\n'[0] {
			cells << piece
			piece = ''
			i++
			continue
		}
		piece += current.str()
		i++
	}
	for j in 0..cells.len {
		cells[j] = cells[j].replace(substr, wrapper.str())
	}
	return cells
}
