macro_rules! archive_format {
    ( $format:ident, $fixture:expr, $open:expr, $close:expr ) => {
        paste::paste! {
            use std::ffi::OsString;
            use std::io::Result;
            use std::path::{Path, PathBuf};
            use std::time::SystemTime;

            use floppy_disk::mem::*;
            use floppy_disk::prelude::*;
            use indexmap::IndexSet;
            use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
            use tokio::pin;
            use tokio::sync::Mutex;
            use tracing::trace;

            pub(crate) struct [< $format InternalMetadata >] {
                pub delegate: MemFloppyDisk,
                pub compression: CompressionType,
                pub ordered_paths: IndexSet<PathBuf>,
            }

            #[derive(Debug)]
            pub struct [< $format FloppyDisk >] {
                delegate: MemFloppyDisk,
                compression: smoosh::CompressionType,
                path: PathBuf,
                ordered_paths: Mutex<IndexSet<PathBuf>>,
            }

            impl [< $format FloppyDisk >] {
                pub async fn open<'a, P: AsRef<Path>>(path: P) -> Result<[< $format FloppyDisk >]> {
                    let path = path.as_ref();
                    let metadata: [< $format InternalMetadata >] = $open(path).await?;
                    Ok(Self {
                        delegate: metadata.delegate,
                        compression: metadata.compression,
                        path: path.to_path_buf(),
                        ordered_paths: Mutex::new(metadata.ordered_paths),
                    })
                }

                pub async fn close(self) -> Result<()> {
                    let ordered_paths = &*self.ordered_paths.lock().await;
                    $close(&self.delegate, &self.path, self.compression, ordered_paths).await
                }

                pub(crate) async fn add_path<P: AsRef<Path> + Send>(&self, path: P) {
                    let path = path.as_ref().to_path_buf();
                    let path = if !path.starts_with("/") {
                        PathBuf::from("/").join(path)
                    } else {
                        path
                    };
                    trace!("adding ordered path: {}", path.display());
                    self.ordered_paths.lock().await.insert(path);
                }

                pub(crate) async fn remove_path<P: AsRef<Path> + Send>(&self, path: P) {
                    let path = path.as_ref().to_path_buf();
                    let path = if !path.starts_with("/") {
                        PathBuf::from("/").join(path)
                    } else {
                        path
                    };
                    trace!("removing ordered path: {}", path.display());
                    self.ordered_paths.lock().await.remove(&path);
                }
            }

            #[async_trait::async_trait]
            impl<'a> FloppyDisk<'a> for [< $format FloppyDisk >] {
                type DirBuilder = [< $format DirBuilder >]<'a>;
                type DirEntry = [< $format DirEntry >];
                type File = [< $format File >];
                type FileType = [< $format FileType >];
                type Metadata = [< $format Metadata >];
                type OpenOptions = [< $format OpenOptions >];
                type Permissions = [< $format Permissions >];
                type ReadDir = [< $format ReadDir >];

                async fn canonicalize<P: AsRef<Path> + Send>(&self, path: P) -> Result<PathBuf> {
                    self.delegate.canonicalize(path).await
                }

                async fn copy<P: AsRef<Path> + Send>(&self, from: P, to: P) -> Result<u64> {
                    {
                        let to = to.as_ref();
                        self.add_path(to).await;
                    }
                    self.delegate.copy(from, to).await
                }

                async fn create_dir<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
                    {
                        let path = path.as_ref();
                        self.add_path(path).await;
                    }
                    self.delegate.create_dir(path).await
                }

                async fn create_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
                    {
                        let mut path = path.as_ref();
                        // Since we collect the parent paths in reverse order,
                        // we have to reverse them to ensure they end up in the
                        // stream in the correct order.
                        let mut parents = vec![path.to_path_buf()];
                        while let Some(parent) = path.parent() {
                            parents.push(parent.into());
                            path = parent;
                        }
                        parents.reverse();
                        for parent in parents {
                            self.add_path(parent).await;
                        }
                    }
                    self.delegate.create_dir_all(path).await
                }

                async fn hard_link<P: AsRef<Path> + Send>(&self, src: P, dst: P) -> Result<()> {
                    {let path = dst.as_ref();self.add_path(path.to_path_buf()).await;}
                    self.delegate.hard_link(src, dst).await
                }

                async fn metadata<P: AsRef<Path> + Send>(&self, path: P) -> Result<Self::Metadata> {
                    self.delegate.metadata(path).await.map([< $format Metadata >])
                }

                async fn read<P: AsRef<Path> + Send>(&self, path: P) -> Result<Vec<u8>> {
                    self.delegate.read(path).await
                }

                async fn read_dir<P: AsRef<Path> + Send>(&self, path: P) -> Result<Self::ReadDir> {
                    self.delegate.read_dir(path).await.map([< $format ReadDir >])
                }

                async fn read_link<P: AsRef<Path> + Send>(&self, path: P) -> Result<PathBuf> {
                    self.delegate.read_link(path).await
                }

                async fn read_to_string<P: AsRef<Path> + Send>(&self, path: P) -> Result<String> {
                    self.delegate.read_to_string(path).await
                }

                async fn remove_dir<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
                    {let path = path.as_ref();self.remove_path(path.to_path_buf()).await;}
                    self.delegate.remove_dir(path).await
                }

                async fn remove_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
                    {
                        let mut path = path.as_ref();
                        while let Some(parent) = path.parent() {
                            self.remove_path(parent).await;
                            path = parent;
                        }
                    }
                    self.delegate.remove_dir_all(path).await
                }

                async fn remove_file<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
                    {
                        let path = path.as_ref();
                        self.remove_path(path.to_path_buf()).await;
                    }
                    self.delegate.remove_file(path).await
                }

                async fn rename<P: AsRef<Path> + Send>(&self, from: P, to: P) -> Result<()> {
                    {
                        let from = from.as_ref();
                        self.remove_path(from).await;
                    }
                    {
                        let to = to.as_ref();
                        self.add_path(to).await;
                    }
                    self.delegate.rename(from, to).await
                }

                async fn set_permissions<P: AsRef<Path> + Send>(
                    &self,
                    path: P,
                    perm: Self::Permissions,
                ) -> Result<()> {
                    self.delegate.set_permissions(path, perm.0).await
                }

                async fn symlink<P: AsRef<Path> + Send>(&self, src: P, dst: P) -> Result<()> {
                    {
                        let dst = dst.as_ref();
                        self.add_path(dst).await;
                    }
                    self.delegate.symlink(src, dst).await
                }

                async fn symlink_metadata<P: AsRef<Path> + Send>(
                    &self,
                    path: P,
                ) -> Result<Self::Metadata> {
                    self.delegate.symlink_metadata(path).await.map([< $format Metadata >])
                }

                async fn try_exists<P: AsRef<Path> + Send>(&self, path: P) -> Result<bool> {
                    self.delegate.try_exists(path).await
                }

                async fn write<P: AsRef<Path> + Send>(
                    &self,
                    path: P,
                    contents: impl AsRef<[u8]> + Send,
                ) -> Result<()> {
                    {
                        let path = path.as_ref();
                        self.add_path(path).await;
                    }
                    self.delegate.write(path, contents).await
                }

                fn new_dir_builder(&'a self) -> Self::DirBuilder {
                    [< $format DirBuilder >] {
                        delegate: self,
                        recursive: false,
                        mode: 0o777,
                    }
                }
            }

            #[async_trait::async_trait]
            impl FloppyDiskUnixExt for [< $format FloppyDisk >] {
                async fn chown<P: Into<PathBuf> + Send>(
                    &self,
                    path: P,
                    uid: u32,
                    gid: u32,
                ) -> Result<()> {
                    self.delegate.chown(path, uid, gid).await
                }
            }

            #[derive(Debug)]
            pub struct [< $format DirBuilder >]<'a> {
                #[doc(hidden)] delegate: &'a [< $format FloppyDisk >],
                #[doc(hidden)] recursive: bool,
                #[doc(hidden)] mode: u32,
            }

            #[async_trait::async_trait]
            impl FloppyDirBuilder for [< $format DirBuilder >]<'_> {
                fn recursive(&mut self, recursive: bool) -> &mut Self {
                    self.recursive = (recursive);
                    self
                }

                async fn create<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
                    if self.recursive {
                        self.delegate.create_dir_all(path).await
                    } else {
                        self.delegate.create_dir(path).await
                    }
                }

                #[cfg(unix)]
                fn mode(&mut self, mode: u32) -> &mut Self {
                    self.mode =(mode);
                    self
                }
            }

            #[derive(Debug)]
            #[repr(transparent)]
            pub struct [< $format DirEntry >](#[doc(hidden)] MemDirEntry);

            #[async_trait::async_trait]
            impl<'a> FloppyDirEntry<'a, [< $format FloppyDisk >]> for [< $format DirEntry >] {
                fn path(&self) -> PathBuf {
                    self.0.path()
                }

                fn file_name(&self) -> OsString {
                    self.0.file_name()
                }

                async fn metadata(&self) -> Result<<[< $format FloppyDisk >] as FloppyDisk<'a>>::Metadata> {
                    self.0.metadata().await.map([< $format Metadata >])
                }

                async fn file_type(&self) -> Result<<[< $format FloppyDisk >] as FloppyDisk<'a>>::FileType> {
                    self.0.file_type().await.map([< $format FileType >])
                }

                #[cfg(unix)]
                fn ino(&self) -> u64 {
                    self.0.ino()
                }
            }

            #[derive(Debug)]
            #[repr(transparent)]
            pub struct [< $format File >](#[doc(hidden)] MemFile);

            #[async_trait::async_trait]
            impl<'a> FloppyFile<'a, [< $format FloppyDisk >]> for [< $format File >] {
                async fn sync_all(&mut self) -> Result<()> {
                    self.0.sync_all().await
                }

                async fn sync_data(&mut self) -> Result<()> {
                    self.0.sync_data().await
                }

                async fn set_len(&mut self, size: u64) -> Result<()> {
                    self.0.set_len(size).await
                }

                async fn metadata(&self) -> Result<<[< $format FloppyDisk >] as FloppyDisk>::Metadata> {
                    self.0.metadata().await.map([< $format Metadata >])
                }

                async fn try_clone(&'a self) -> Result<Box<<[< $format FloppyDisk >] as FloppyDisk>::File>> {
                    Ok(Box::new([< $format File >](*self.0.try_clone().await?)))
                }

                async fn set_permissions(
                    &self,
                    perm: <[< $format FloppyDisk >] as FloppyDisk>::Permissions,
                ) -> Result<()> {
                    Ok(self.0.set_permissions(perm.0).await?)
                }

                async fn permissions(&self) -> Result<<[< $format FloppyDisk >] as FloppyDisk>::Permissions> {
                    self.0.permissions().await.map([< $format Permissions >])
                }
            }

            impl AsyncRead for [< $format File >] {
                fn poll_read(
                    self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                    buf: &mut tokio::io::ReadBuf<'_>,
                ) -> std::task::Poll<std::io::Result<()>> {
                    let this = self.get_mut();
                    let delegate = &mut this.0;
                    pin!(delegate);
                    AsyncRead::poll_read(delegate, cx, buf)
                }
            }

            impl AsyncWrite for [< $format File >] {
                fn poll_write(
                    self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                    buf: &[u8],
                ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
                    let this = self.get_mut();
                    let delegate = &mut this.0;
                    pin!(delegate);
                    AsyncWrite::poll_write(delegate, cx, buf)
                }

                fn poll_flush(
                    self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
                    let this = self.get_mut();
                    let delegate = &mut this.0;
                    pin!(delegate);
                    AsyncWrite::poll_flush(delegate, cx)
                }

                fn poll_shutdown(
                    self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
                    let this = self.get_mut();
                    let delegate = &mut this.0;
                    pin!(delegate);
                    AsyncWrite::poll_shutdown(delegate, cx)
                }
            }

            impl AsyncSeek for [< $format File >] {
                fn start_seek(
                    self: std::pin::Pin<&mut Self>,
                    position: std::io::SeekFrom,
                ) -> std::io::Result<()> {
                    let this = self.get_mut();
                    let delegate = &mut this.0;
                    pin!(delegate);
                    AsyncSeek::start_seek(delegate, position)
                }

                fn poll_complete(
                    self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<std::io::Result<u64>> {
                    let this = self.get_mut();
                    let delegate = &mut this.0;
                    pin!(delegate);
                    AsyncSeek::poll_complete(delegate, cx)
                }
            }

            #[derive(Debug)]
            #[repr(transparent)]
            pub struct [< $format FileType >](#[doc(hidden)] MemFileType);

            #[async_trait::async_trait]
            impl FloppyFileType for [< $format FileType >] {
                fn is_dir(&self) -> bool {
                    self.0.is_dir()
                }

                fn is_file(&self) -> bool {
                    self.0.is_file()
                }

                fn is_symlink(&self) -> bool {
                    self.0.is_symlink()
                }
            }

            #[derive(Debug)]
            #[repr(transparent)]
            pub struct [< $format Metadata >](#[doc(hidden)] MemMetadata);

            impl<'a> FloppyMetadata<'a, [< $format FloppyDisk >]> for [< $format Metadata >] {
                fn file_type(&self) -> <[< $format FloppyDisk >] as FloppyDisk<'a>>::FileType {
                    [< $format FileType >](self.0.file_type())
                }

                fn is_dir(&self) -> bool {
                    self.0.is_dir()
                }

                fn is_file(&self) -> bool {
                    self.0.is_file()
                }

                fn is_symlink(&self) -> bool {
                    self.0.is_symlink()
                }

                fn len(&self) -> u64 {
                    self.0.len()
                }

                fn permissions(&self) -> <[< $format FloppyDisk >] as FloppyDisk<'a>>::Permissions {
                    [< $format Permissions >](self.0.permissions())
                }

                fn modified(&self) -> Result<SystemTime> {
                    self.0.modified()
                }

                fn accessed(&self) -> Result<SystemTime> {
                    self.0.accessed()
                }

                fn created(&self) -> Result<SystemTime> {
                    self.0.created()
                }
            }

            impl FloppyUnixMetadata for [< $format Metadata >] {
                fn uid(&self) -> Result<u32> {
                    self.0.uid()
                }

                fn gid(&self) -> Result<u32> {
                    self.0.gid()
                }
            }

            #[derive(Debug)]
            pub struct [< $format OpenOptions>](#[doc(hidden)] MemOpenOptions, #[doc(hidden)] bool);

            #[async_trait::async_trait]
            impl<'a> FloppyOpenOptions<'a, [< $format FloppyDisk >]> for [< $format OpenOptions >] {
                fn new() -> Self {
                    Self(MemOpenOptions::new(), false)
                }

                fn read(self, read: bool) -> Self {
                    Self(self.0.read(read), self.1)
                }

                fn write(self, write: bool) -> Self {
                    Self(self.0.write(write), self.1)
                }

                fn append(self, append: bool) -> Self {
                    Self(self.0.append(append), self.1)
                }

                fn truncate(self, truncate: bool) -> Self {
                    Self(self.0.truncate(truncate), self.1)
                }

                fn create(self, create: bool) -> Self {
                    Self(self.0.create(create), create)
                }

                fn create_new(self, create_new: bool) -> Self {
                    Self(self.0.create_new(create_new), create_new)
                }

                async fn open<P: AsRef<Path> + Send>(
                    &self,
                    disk: &'a [< $format FloppyDisk >],
                    path: P,
                ) -> Result<<[< $format  FloppyDisk >] as FloppyDisk<'a>>::File> {
                    if self.1 {
                        let path = path.as_ref();
                        disk.add_path(path).await;
                    }
                    self.0.open(&disk.delegate, path).await.map([< $format File >])
                }
            }

            #[derive(Debug)]
            #[repr(transparent)]
            pub struct [< $format Permissions >](#[doc(hidden)] MemPermissions);

            impl FloppyPermissions for [< $format Permissions >] {
                fn readonly(&self) -> bool {
                    self.0.readonly()
                }

                fn set_readonly(&mut self, readonly: bool) {
                    self.0.set_readonly(readonly)
                }
            }

            impl FloppyUnixPermissions for [< $format Permissions >] {
                fn mode(&self) -> u32 {
                    FloppyUnixPermissions::mode(&self.0)
                }

                fn set_mode(&mut self, mode: u32) {
                    FloppyUnixPermissions::set_mode(&mut self.0, mode)
                }

                fn from_mode(mode: u32) -> Self {
                    Self(MemPermissions::from_mode(mode))
                }
            }

            #[derive(Debug)]
            #[repr(transparent)]
            pub struct [< $format ReadDir >](#[doc(hidden)] MemReadDir);

            #[async_trait::async_trait]
            impl<'a> FloppyReadDir<'a, [< $format FloppyDisk >]> for [< $format ReadDir >] {
                async fn next_entry(
                    &mut self,
                ) -> Result<Option<<[< $format FloppyDisk >] as FloppyDisk<'a>>::DirEntry>> {
                    self.0.next_entry().await.map(|e| e.map([< $format DirEntry >]))
                }
            }

            #[cfg(test)]
            mod tests {
                use super::*;
                use std::io::Result;

                #[test_log::test(tokio::test)]
                async fn test_read_works() -> Result<()> {
                    let archive = crate::util::tests::TempFile::new(concat!("./fixtures/", $fixture)).await?;
                    let disk = [< $format FloppyDisk >]::open(archive.path_view()).await?;

                    let input = disk.read_to_string("/a.txt").await?;
                    assert_eq!("asdf\n", input);
                    disk.close().await?;

                    Ok(())
                }

                #[test_log::test(tokio::test)]
                async fn test_write_works() -> Result<()> {
                    let archive = crate::util::tests::TempFile::new(concat!("./fixtures/", $fixture)).await?;
                    {
                        let disk = [< $format FloppyDisk >]::open(archive.path_view()).await?;
                        disk.write("/b.txt", "wow!!!").await?;
                        disk.close().await?;
                    }
                    {
                        let disk = [< $format FloppyDisk >]::open(archive.path_view()).await?;

                        let input = disk.read_to_string("/b.txt").await?;
                        assert_eq!("wow!!!", input);
                        disk.close().await?;
                    }

                    Ok(())
                }

                #[test_log::test(tokio::test)]
                async fn test_directories_works() -> Result<()> {
                    let archive = crate::util::tests::TempFile::new(concat!("./fixtures/", $fixture)).await?;
                    {
                        let disk = [< $format FloppyDisk >]::open(archive.path_view()).await?;
                        disk.create_dir_all("/test/thing").await?;
                        disk.write("/test/thing/heck.txt", "omg!!!").await?;
                        disk.close().await?;
                    }
                    {
                        let disk = [< $format FloppyDisk >]::open(archive.path_view()).await?;

                        let input = disk.read_to_string("/test/thing/heck.txt").await?;
                        assert_eq!("omg!!!", input);
                        disk.close().await?;
                    }

                    Ok(())
                }

                #[test_log::test(tokio::test)]
                async fn test_many_files_and_directories_works() -> Result<()> {
                    let archive = crate::util::tests::TempFile::new(concat!("./fixtures/", $fixture)).await?;
                    {
                        let disk = [< $format FloppyDisk >]::open(archive.path_view()).await?;
                        disk.create_dir_all("/test/thing").await?;
                        disk.create_dir_all("/a/b/c/d/e/f/g").await?;
                        disk.create_dir_all("/1/2/3/4/5").await?;

                        disk.write("/test/thing/heck.txt", "omg!!!").await?;
                        disk.write("/a/b/c/d/e/f/g/h.txt", "gasp!!!").await?;
                        disk.write("/1/2/3/4/5/6.txt", "wtf!!!").await?;
                        disk.close().await?;
                    }
                    {
                        let disk = [< $format FloppyDisk >]::open(archive.path_view()).await?;

                        let input = disk.read_to_string("/test/thing/heck.txt").await?;
                        assert_eq!("omg!!!", input);

                        let input = disk.read_to_string("/a/b/c/d/e/f/g/h.txt").await?;
                        assert_eq!("gasp!!!", input);

                        let input = disk.read_to_string("/1/2/3/4/5/6.txt").await?;
                        assert_eq!("wtf!!!", input);
                        disk.close().await?;
                    }

                    Ok(())
                }
            }
        }
    };
}

use std::path::{Path, PathBuf};

pub(crate) use archive_format;
use tracing::debug;

pub(crate) async fn exists_async<P: AsRef<Path>>(path: P) -> bool {
    let path = path.as_ref();
    tokio::fs::canonicalize(path).await.is_ok()
}

pub(crate) async fn async_file<P: AsRef<Path>>(path: P) -> std::io::Result<tokio::fs::File> {
    let path = path.as_ref();
    let path = tokio::fs::canonicalize(path).await?;
    debug!("open file async: {}", path.display());
    let file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .await?;
    file.sync_all().await?;
    Ok(file)
}

pub(crate) struct TempDir {
    path: PathBuf,
}

#[allow(unused)]
impl TempDir {
    pub async fn new() -> std::io::Result<TempDir> {
        let mut path = std::env::temp_dir();
        path.push(format!("flop-workdir-{}", rand::random::<u64>()));
        debug!("creating tempdir: {}", path.display());
        tokio::fs::create_dir_all(&path).await?;

        Ok(TempDir { path })
    }

    pub fn path_view(&self) -> PathBuf {
        self.path.clone()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        debug!("dropping {}!", self.path.display());
        if self.path.exists() {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<PathBuf> for TempDir {
    fn as_ref(&self) -> &PathBuf {
        &self.path
    }
}

impl std::ops::Deref for TempDir {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

#[cfg(test)]
#[allow(unused)]
pub(crate) mod tests {
    use tracing::debug;

    use super::TempDir;
    use std::path::{Path, PathBuf};

    pub(crate) struct TempFile {
        scope: TempDir,
        path: PathBuf,
    }

    impl TempFile {
        pub async fn new<P: AsRef<Path>>(fixture: P) -> std::io::Result<TempFile> {
            let fixture = fixture.as_ref();
            let scope = TempDir::new().await?;
            let file = fixture.file_name().unwrap();
            let mut path = scope.path_view().to_path_buf();
            path.push(file);
            tokio::io::copy(
                &mut tokio::fs::File::open(fixture).await?,
                &mut tokio::fs::File::create(&path).await?,
            )
            .await?;

            Ok(TempFile { scope, path })
        }

        pub fn path_view(&self) -> &Path {
            &self.path
        }

        pub fn scope_view(&self) -> &Path {
            &self.scope
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            debug!("dropping tempfile {}!", &self.path.display());
        }
    }
}
