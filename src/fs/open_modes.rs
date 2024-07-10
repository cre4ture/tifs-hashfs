#[derive(PartialEq, Eq)]
pub enum OpenMode {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

impl OpenMode {
    pub fn allows_write(&self) -> bool {
        *self != Self::ReadOnly
    }
}
