
use fuser::MountOption as FuseMountOption;
use paste::paste;
use tracing::{debug, error};
use parse_size::parse_size;

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
}}

#[derive(Clone)]
pub struct TiFsConfig {
    pub key_prefix: Vec<u8>,
    pub block_size: u64,
    pub enable_atime: bool,
    pub enable_mtime: bool,
    pub direct_io: bool,
    pub inode_cache_size: u64,
    pub hashed_blocks: bool,
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

    pub fn from_options(options: &Vec<MountOption>) -> Self {

        let name_str = options.iter().find_map(|opt|{
            if let MountOption::Name(name) = opt {
                Some(name.clone())
            } else {
                None
            }
        }).unwrap_or("".into());

        let block_size = options
        .iter()
        .find_map(|option| match option {
            MountOption::BlkSize(size) => parse_size(size)
                .map_err(|err| {
                    error!("fail to parse blksize({}): {}", size, err);
                    err
                })
                .map(|size| {
                    debug!("block size: {}", size);
                    size
                })
                .ok(),
            _ => None,
        })
        .unwrap_or(Self::DEFAULT_BLOCK_SIZE);

        TiFsConfig {
            key_prefix: name_str.into_bytes(),
            block_size,
            direct_io: options.iter().any(|option| matches!(option, MountOption::DirectIO)),
            max_size: options.iter().find_map(|option| match option {
                MountOption::MaxSize(size) => parse_size(size)
                    .map_err(|err| {
                        error!("fail to parse maxsize({}): {}", size, err);
                        err
                    })
                    .map(|size| {
                        debug!("max size: {}", size);
                        size
                    })
                    .ok(),
                _ => None,
            }),
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
        }
    }
}
