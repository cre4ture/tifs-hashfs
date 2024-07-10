/*
use std::{collections::BTreeMap, path::PathBuf};




struct PersistentTree {
    root: PathBuf,
    tree: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl PersistentTree {
    pub fn new(root: PathBuf) -> Self {
        let me = Self {
            root,
            tree: BTreeMap::new(),
        };

        //let mut path_stack = root.clone();
        //let mut stack = Vec::with_capacity(256);
        //me.load_all(&mut root, &mut stack)
        me
    }
    fn load_all(&mut self, dir: &mut PathBuf, prefix: &mut Vec<u8>) -> Result<()> {
        let list = read_dir(dir)?;
        for entry in list.into_iter() {
            if let Ok(f) = entry {
                if let Ok(t) = f.file_type() {
                    if let Ok(filename) = f.file_name().into_string() {
                        dir.push(f.file_name());
                        let original_len = prefix.len();
                        prefix.extend_from_slice(filename.as_bytes());
                        match t {
                            t if t.is_dir() => self.load_all(dir, prefix)?,
                            t if t.is_file() => self.load_file(&dir, prefix),
                            _ => {}
                        }
                        self.load_all(dir, prefix)?;
                        prefix.shrink_to(original_len);
                        dir.pop();
                    }
                }
            }
        }
        Ok(())
    }

    fn load_file(&mut self, path: &PathBuf, prefix: &[u8]) {
        std::fs::read
    }
}
    */
