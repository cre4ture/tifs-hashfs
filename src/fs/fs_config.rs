

use fuser::MountOption as FuseMountOption;
use paste::paste;
use tracing::error;
use parse_size::parse_size;

use super::{error::{FsError, TiFsResult}, inode::TiFsHash, utils::hash_algorithm::{HashAlgorithm, ALGO_HASH_LEN_MAP, ALGO_NAME_MAP}};


macro_rules! define_options {
    {
        $name: ident ($type: ident) {
            $(builtin $($optname: literal)? $opt: ident,)*
            $(define $($newoptname: literal)? $newopt: ident $( ( $optval: ident ) )? ,)*
        }
    } =>
    {
        #[derive(Debug,Clone,PartialEq)]
        pub enum $name {
            Unknown(String),
            $($opt,)*
            $($newopt $(($optval))?,)*
        }
        impl $name {
            pub fn to_vec<'a, I: Iterator<Item=&'a str>>(iter: I) -> Vec<Self> {
                iter.map(|v| v.split(',').map(Self::from)).flatten().collect()
            }
            pub fn collect_builtin<'a, I: Iterator<Item=&'a Self>>(iter: I) -> Vec<$type> {
                iter.filter_map(|v| v.to_builtin()).collect()
            }
            pub fn to_builtin(&self) -> Option<$type> {
                match self {
                    $(Self::$opt => Some($type::$opt),)*
                    _ => None,
                }
            }
        }
        paste! {
            impl std::str::FromStr for $name {
                type Err = anyhow::Error;
                fn from_str(fullopt: &str) -> Result<Self, Self::Err> {
                    let mut splitter = fullopt.splitn(2, '=');
                    let optname = splitter.next().unwrap_or("");
                    let optval = splitter.next();
                    let optval_present = optval.is_some();
                    let optval = optval.unwrap_or("");

                    let (parsed, optval_used) = match &optname as &str {
                        // "dirsync" => ( Self::DirSync, false),
                        // "direct_io" if "" != "directio" => ( Self::DirectIO, false),
                        // "blksize" => ( Self::BlkSize ( "0".parse::<u64>()? , false || (None as Option<u64>).is_none() ),
                        $( $($optname if "" != )? stringify!([<$opt:lower>]) => (Self::$opt, false), )*
                        $(
                            $($newoptname if "" != )? stringify!([<$newopt:lower>]) => (
                                Self::$newopt $(( optval.parse::<$optval>()?))? , false $( || (None as Option<$optval>).is_none() )?
                            ),
                        )*
                        _ => (Self::Unknown(fullopt.to_owned()), false),
                    };

                    if !optval_used && optval_present {
                        Err(anyhow::anyhow!("Option {} do not accept an argument", optname))
                    } else if optval_used && !optval_present {
                        Err(anyhow::anyhow!("Argument for {} is not supplied", optname))
                    } else {
                        Ok(parsed)
                    }
                }
            }
            impl<T> From<T> for $name
            where
                T: ToString
            {
                fn from(v: T) -> Self {
                    let fullopt = v.to_string();
                    match fullopt.parse::<Self>() {
                        Ok(v) => v,
                        Err(_) => Self::Unknown(v.to_string()),
                    }
                }
            }
            impl From<$name> for String {
                fn from(v: $name) -> Self {
                    Self::from(&v)
                }
            }
            impl From<&$name> for String {
                fn from(v: &$name) -> Self {
                    match v {
                        // MountOption::DirSync => ("dirsync", "").0.to_owned() ,
                        // MountOption::DirectIO => format!(concat!("{}"), ("direct_io", "directio", "").0 ),
                        // MountOption::BlkSize (v) => format!(concat!("{}", "={}", ""), ("blksize", "").0, v.to_owned() as u64 ),
                        $($name::$opt => ( $($optname,)? stringify!([<$opt:lower>]), "" ).0 .to_owned() , )*
                        $(
                            $name::$newopt $( ( define_options!(@ignore $optval v) ) )? =>
                                format!(
                                    concat!("{}" $(,"={}", define_options!(@ignore $optval) )? ),
                                    ( $($newoptname,)? stringify!([<$newopt:lower>]), "" ).0
                                    $( , v.to_owned() as $optval )?
                                ),
                        )*
                        $name::Unknown(v) => v.to_owned(),
                    }
                }
            }
        }
    };

    // internal rules
    {@ignore $id: tt } => { "" };
    {@ignore $id: tt $($replacement: tt),* } => { $($replacement),* };
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_mount_options() {
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["direct_io", "nodev,exec"].iter().copied())
            ),
            "[DirectIO, NoDev, Exec]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["direct_io="].iter().copied())
            ),
            "[Unknown(\"direct_io=\")]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["direct_io=1"].iter().copied())
            ),
            "[Unknown(\"direct_io=1\")]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["direct_io=1=2"].iter().copied())
            ),
            "[Unknown(\"direct_io=1=2\")]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["undefined"].iter().copied())
            ),
            "[Unknown(\"undefined\")]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["undefined=foo"].iter().copied())
            ),
            "[Unknown(\"undefined=foo\")]"
        );
        assert_eq!(
            format!("{:?}", MountOption::to_vec(vec!["dev="].iter().copied())),
            "[Unknown(\"dev=\")]"
        );
        assert_eq!(
            format!("{:?}", MountOption::to_vec(vec!["dev=1"].iter().copied())),
            "[Unknown(\"dev=1\")]"
        );
        assert_eq!(
            format!("{:?}", MountOption::to_vec(vec!["blksize"].iter().copied())),
            "[Unknown(\"blksize\")]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["blksize="].iter().copied())
            ),
            "[Unknown(\"blksize=\")]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["blksize=32"].iter().copied())
            ),
            "[BlkSize(32)]"
        );
        assert_eq!(
            format!("{:?}", MountOption::to_vec(vec!["tls"].iter().copied())),
            "[Unknown(\"tls\")]"
        );
        assert_eq!(
            format!("{:?}", MountOption::to_vec(vec!["tls="].iter().copied())),
            "[Tls(\"\")]"
        );
        assert_eq!(
            format!("{:?}", MountOption::to_vec(vec!["tls=xx"].iter().copied())),
            "[Tls(\"xx\")]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["tls=/root"].iter().copied())
            ),
            "[Tls(\"/root\")]"
        );
        assert_eq!(
            format!(
                "{:?}",
                MountOption::to_vec(vec!["direct_io", "nodev,blksize=32"].iter().copied())
            ),
            "[DirectIO, NoDev, BlkSize(32)]"
        );
    }

    #[test]
    fn convert_mount_options() {
        assert_eq!(
            MountOption::NoDev.to_builtin(),
            Some(FuseMountOption::NoDev)
        );
        assert_eq!(
            MountOption::DirSync.to_builtin(),
            Some(FuseMountOption::DirSync)
        );
        assert_eq!(MountOption::DirectIO.to_builtin(), None);
        assert_eq!(MountOption::BlkSize("1".to_owned()).to_builtin(), None);
        assert_eq!(MountOption::MaxSize("1".to_owned()).to_builtin(), None);
    }

    #[test]
    fn format_mount_options() {
        assert_eq!(String::from(MountOption::NoDev), "nodev");
        assert_eq!(String::from(MountOption::DirectIO), "direct_io");
        assert_eq!(
            String::from(MountOption::BlkSize("123".to_owned())),
            "blksize=123"
        );
        assert_eq!(
            String::from(MountOption::BlkSize("1MiB".to_owned())),
            "blksize=1MiB"
        );
    }
}


define_options! { MountOption (FuseMountOption) {
    builtin Dev,
    builtin NoDev,
    builtin Suid,
    builtin NoSuid,
    builtin RO,
    builtin RW,
    builtin Exec,
    builtin NoExec,
    builtin DirSync,
    builtin Atime,
    builtin NoAtime,
    define "direct_io" DirectIO,
    define BlkSize(String),
    define MaxSize(String), // size of filesystem
    define Tls(String),
//    define "opt" OptionName(Display_Debug_Clone_PartialEq_FromStr_able)
    define Name(String),
    define InodeCacheSize(String),
    define HashedBlocks,
    define HashedBlocksCacheSize(String),
    define NoMtime,
    define ValidateWrites,
    define ValidateReadHashes,
    define RawHashedBlocks,
    define BatchRawBlockWrite,
    define PureRaw,
    define NoExistenceCheck,
    define ParallelJobs(String),
    define MaxChunkSize(String),
    define WriteInProgressLimit(String),
    define ReadAheadSize(String),
    define ReadAheadInProgressLimit(String),
    define HashAlgorithm(String),
    define InlineDataLimit(String),
}}

#[derive(Clone)]
pub struct TiFsConfig {
    pub key_prefix: Vec<u8>,
    pub block_size: u64,
    pub inline_data_limit: u64,
    pub enable_atime: bool,
    pub enable_mtime: bool,
    pub direct_io: bool,
    pub inode_cache_size: u64,
    pub hashed_blocks: bool,
    pub hash_algorithm: HashAlgorithm,
    pub hash_len: usize,
    pub hashed_blocks_cache_size: u64,
    pub max_size: Option<u64>,
    pub validate_writes: bool,
    pub validate_read_hashes: bool,
    pub raw_hashed_blocks: bool,
    pub batch_raw_block_write: bool,
    pub pure_raw: bool,
    pub existence_check: bool,
    pub parallel_jobs: usize,
    pub max_chunk_size: usize,
    pub write_in_progress_limit: usize,
    pub read_ahead_size: u64,
    pub read_ahead_in_progress_limit: usize,
}

impl TiFsConfig {
    pub const DEFAULT_BLOCK_SIZE: u64 = 1 << 16;

    pub fn from_options(options: &Vec<MountOption>) -> TiFsResult<Self> {

        // first declare params and set default value
        let mut name = String::from("");
        let mut max_size = None;
        let mut block_size = Self::DEFAULT_BLOCK_SIZE;
        let mut hash_algo = HashAlgorithm::Blake3;
        const INLINE_DATA_THRESHOLD_BASE: u64 = 1 << 4;
        let mut inline_data_limit = block_size / INLINE_DATA_THRESHOLD_BASE;

        // iterate over options and overwrite defaults
        for option in options {
            match option {
                MountOption::Name(n) => name = n.clone(),
                MountOption::BlkSize(size) => {
                    block_size = parse_size(size)
                        .map_err(|err| {
                            FsError::ConfigParsingFailed {
                                msg: format!("failed to parse blksize({}): {}", size, err) }
                        })?;
                }
                MountOption::MaxSize(size) => {
                    max_size = Some(parse_size(size)
                        .map_err(|err| {
                            FsError::ConfigParsingFailed {
                                msg: format!("failed to parse maxsize({}): {}", size, err) }
                        })?);
                }
                MountOption::HashAlgorithm(value) => {
                    hash_algo = *ALGO_NAME_MAP.get_by_left(value.as_str()).ok_or(
                        FsError::ConfigParsingFailed {
                            msg: format!("hash algo name unsupported: {}", value)
                        }
                    )?;
                }
                MountOption::InlineDataLimit(value) => {
                    inline_data_limit = parse_size(value)
                        .map_err(|err| {
                            FsError::ConfigParsingFailed {
                                msg: format!("failed to parse InlineDataLimit({}): {}", value, err) }
                        })?;
                }
                _ => {}
            }
        }

        // calculate derived parameters
        let hash_len = *ALGO_HASH_LEN_MAP.get(&hash_algo).unwrap();

        let cfg = TiFsConfig {
            key_prefix: name.into_bytes(),
            block_size,
            inline_data_limit,
            direct_io: options.iter().any(|option| matches!(option, MountOption::DirectIO)),
            max_size,
            enable_atime: options.iter().find_map(|option| match option {
                MountOption::NoAtime => Some(false),
                MountOption::Atime => Some(true),
                _ => None,
            }).unwrap_or(true),
            enable_mtime: options.iter().find_map(|option| match option {
                MountOption::NoMtime => Some(false),
                _ => None,
            }).unwrap_or(true),
            inode_cache_size: options.iter().find_map(|opt|{
                if let MountOption::InodeCacheSize(value) = &opt {
                    parse_size(value).map_err(|err|{
                        error!("fail to parse InodeCacheSize({}): {}", value, err);
                    }).ok()
                } else { None }
            }).unwrap_or(10 << 20),
            hashed_blocks: options.iter().find_map(|opt|{
                (MountOption::HashedBlocks == *opt).then_some(true)
            }).unwrap_or(false),
            hash_algorithm: hash_algo,
            hash_len: hash_len,
            hashed_blocks_cache_size: options.iter().find_map(|opt|{
                if let MountOption::HashedBlocksCacheSize(value) = &opt {
                    parse_size(value).map_err(|err|{
                        error!("fail to parse HashedBlocksCacheSize({}): {}", value, err);
                    }).ok()
                } else { None }
            }).unwrap_or(200 << 20),
            validate_writes: options.iter().find_map(|opt|{
                (MountOption::ValidateWrites == *opt).then_some(true)
            }).unwrap_or(false),
            validate_read_hashes: options.iter().find_map(|opt|{
                (MountOption::ValidateReadHashes == *opt).then_some(true)
            }).unwrap_or(false),
            raw_hashed_blocks: options.iter().find_map(|opt|{
                (MountOption::RawHashedBlocks == *opt).then_some(true)
            }).unwrap_or(false),
            batch_raw_block_write: options.iter().find_map(|opt|{
                (MountOption::BatchRawBlockWrite == *opt).then_some(true)
            }).unwrap_or(false),
            pure_raw: options.iter().find_map(|opt|{
                (MountOption::PureRaw == *opt).then_some(true)
            }).unwrap_or(false),
            existence_check: options.iter().find_map(|opt|{
                (MountOption::NoExistenceCheck == *opt).then_some(false)
            }).unwrap_or(true),
            parallel_jobs: options.iter().find_map(|opt|{
                if let MountOption::ParallelJobs(value) = &opt {
                    value.parse::<usize>().map_err(|err|{
                        error!("fail to parse ParallelJobs({}): {}", value, err);
                    }).ok()
                } else { None }
            }).unwrap_or(1),
            max_chunk_size: options.iter().find_map(|opt|{
                if let MountOption::MaxChunkSize(value) = &opt {
                    value.parse::<usize>().map_err(|err|{
                        error!("fail to parse MaxChunkSize({}): {}", value, err);
                    }).ok()
                } else { None }
            }).unwrap_or(usize::MAX),
            write_in_progress_limit: options.iter().find_map(|opt|{
                if let MountOption::WriteInProgressLimit(value) = &opt {
                    value.parse::<usize>().map_err(|err|{
                        error!("fail to parse WriteInProgressLimit({}): {}", value, err);
                    }).ok()
                } else { None }
            }).unwrap_or(0),
            read_ahead_size: options.iter().find_map(|opt|{
                if let MountOption::ReadAheadSize(value) = &opt {
                    parse_size(value).map_err(|err|{
                        error!("fail to parse WriteInProgressLimit({}): {}", value, err);
                    }).ok()
                } else { None }
            }).unwrap_or(0),
            read_ahead_in_progress_limit: options.iter().find_map(|opt|{
                if let MountOption::ReadAheadInProgressLimit(value) = &opt {
                    value.parse::<usize>().map_err(|err|{
                        error!("fail to parse ReadAheadInProgressLimit({}): {}", value, err);
                    }).ok()
                } else { None }
            }).unwrap_or(10),
        };
        Ok(cfg)
    }

    pub fn calculate_hash(&self, input: &[u8]) -> TiFsHash {
        self.hash_algorithm.calculate_hash(input)
    }
}
