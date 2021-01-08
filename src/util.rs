pub fn retain_last_by_key<T, F, K>(slice: &mut Vec<T>, mut key_fn: F)
where
    F: FnMut(&T) -> K,
    K: Eq,
{
    if slice.len() == 0 {
        return;
    }

    let mut current_key = key_fn(&slice[0]);
    let mut write_idx = 0;
    let mut move_from_idx = 0;
    let mut read_idx = 1;
    while read_idx < slice.len() {
        let new_key = key_fn(&slice[read_idx]);
        if new_key != current_key {
            current_key = new_key;

            if move_from_idx != write_idx {
                slice.swap(move_from_idx, write_idx);
            }
            write_idx += 1;
        }
        move_from_idx += 1;
        read_idx += 1;
    }
    if move_from_idx != write_idx {
        slice.swap(move_from_idx, write_idx);
    }

    slice.truncate(write_idx + 1);
}
