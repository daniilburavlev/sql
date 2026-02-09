use crate::error::DbError;

pub mod error;

pub trait Pageable: Sized {
    fn write(&self, buffer: &mut [u8]) -> Result<usize, DbError>;

    fn read(buffer: &[u8]) -> Result<(Self, usize), DbError>;

    fn size(&self) -> usize;
}

#[macro_export]
macro_rules! read_num {
    ($buffer:expr, $ty:ty) => {{
        const SIZE: usize = std::mem::size_of::<$ty>();
        let mut value = [0u8; SIZE];
        value.copy_from_slice(&$buffer[..SIZE]);
        <$ty>::from_be_bytes(value)
    }};
    ($buffer:expr, $ty:ty, $offset:expr) => {{
        const SIZE: usize = std::mem::size_of::<$ty>();
        let mut value = [0u8; SIZE];
        value.copy_from_slice(&$buffer[$offset..$offset + SIZE]);
        <$ty>::from_be_bytes(value)
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn read_i32_num() {
        let num: i32 = 16;
        let bytes = num.to_be_bytes();
        assert_eq!(num, read_num!(bytes, i32));
    }
}
