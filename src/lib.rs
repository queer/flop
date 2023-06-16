pub mod prelude {
    pub mod ar {
        pub use crate::ar::*;
    }
    pub mod cpio {
        pub use crate::cpio::*;
    }
    pub mod tar {
        pub use crate::tar::*;
    }
    pub mod zip {
        pub use crate::zip::*;
    }
}

pub mod ar;
pub mod cpio;
pub mod tar;
pub mod zip;

pub(crate) mod util;
