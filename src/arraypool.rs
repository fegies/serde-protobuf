use crate::value::borrowed::{Field, RepeatedFieldValue, SingleFieldValue};

pub struct ArrayPool {
    single_field_val_vec_pool: Vec<Vec<SingleFieldValue<'static>>>,
    field_vec_pool: Vec<Vec<Field<'static, SingleFieldValue<'static>>>>,
    repeated_field_vec_pool: Vec<Vec<Field<'static, RepeatedFieldValue<'static>>>>,
}

impl std::fmt::Debug for ArrayPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayPool").finish()
    }
}

unsafe fn clear_vec<T, R>(mut vec: Vec<T>) -> Vec<R> {
    vec.clear();
    let mut vec = std::mem::ManuallyDrop::new(vec);
    Vec::from_raw_parts(vec.as_mut_ptr() as *mut R, vec.len(), vec.capacity())
}

impl ArrayPool {
    pub fn new() -> Self {
        ArrayPool {
            single_field_val_vec_pool: Vec::new(),
            field_vec_pool: Vec::new(),
            repeated_field_vec_pool: Vec::new(),
        }
    }
    pub fn get_single_field_val_vec<'a>(&mut self) -> Vec<SingleFieldValue<'a>> {
        self.single_field_val_vec_pool
            .pop()
            .unwrap_or_else(Vec::new)
    }
    pub fn return_single_field_val_vec(&mut self, value: Vec<SingleFieldValue<'_>>) {
        self.single_field_val_vec_pool
            .push(unsafe { clear_vec(value) });
    }

    pub fn get_field_vec<'a>(&mut self) -> Vec<Field<'a, SingleFieldValue<'a>>> {
        self.field_vec_pool.pop().unwrap_or_else(Vec::new)
    }

    pub fn return_field_vec(&mut self, value: Vec<Field<'_, SingleFieldValue<'_>>>) {
        self.field_vec_pool.push(unsafe { clear_vec(value) });
    }

    pub fn get_repeated_field_vec<'a>(&mut self) -> Vec<Field<'a, RepeatedFieldValue<'a>>> {
        self.repeated_field_vec_pool.pop().unwrap_or_else(Vec::new)
    }

    pub fn return_repeated_field_vec(&mut self, value: Vec<Field<'_, RepeatedFieldValue<'_>>>) {
        self.repeated_field_vec_pool
            .push(unsafe { clear_vec(value) });
    }
}
