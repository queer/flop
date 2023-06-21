use std::os::unix::prelude::OsStringExt;

use futures::TryStreamExt;
use smoosh::CompressionType;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_tar_up2date::EntryType;
use tracing::{debug, warn};

crate::util::archive_format!(Tar, "a.tar", tar_open, tar_close);

async fn tar_open<P: Into<PathBuf>>(path: P) -> Result<TarInternalMetadata> {
    let path = path.into();
    debug!("considering {}...", path.display());
    if !crate::util::exists_async(path.clone()).await {
        debug!("nah, just empty tar: {}", path.display());
        let _archive = tokio_tar_up2date::Builder::new(File::create(path).await?);
        return Ok(TarInternalMetadata {
            delegate: MemFloppyDisk::new(),
            compression: CompressionType::None,
            ordered_paths: IndexSet::new(),
        });
    }

    debug!("opening tar file {}", path.display());
    let mut file = crate::util::async_file(path).await?;
    let mut buffer = vec![];
    let c = smoosh::recompress(&mut file, &mut buffer, smoosh::CompressionType::None).await?;
    let mut archive = tokio_tar_up2date::Archive::new(buffer.as_slice());
    let out = MemFloppyDisk::new();
    let mut ordered_paths = IndexSet::new();
    out.create_dir_all("/").await?;

    let mut entries = archive.entries()?;
    while let Some(mut entry) = entries.try_next().await? {
        debug!("reading header...");
        let header = entry.header();
        let path = PathBuf::from(OsString::from_vec(header.path_bytes().as_ref().to_vec()));
        debug!("processing archive path {}", path.display());
        ordered_paths.insert(path.clone());

        if header.entry_type().is_dir() {
            debug!("creating: {}", path.display());
            out.create_dir_all(&path).await?;

            out.chown(&path, header.uid()? as u32, header.gid()? as u32)
                .await?;
            out.set_permissions(&path, MemPermissions::from_mode(header.mode()?))
                .await?;
        } else if header.entry_type().is_file() {
            if let Some(parent) = path.parent() {
                debug!("creating parent(s): {}", parent.display());
                out.create_dir_all(parent).await?;
            }
            debug!("open: {}", path.display());
            let mut handle = MemOpenOptions::new()
                .create(true)
                .write(true)
                .open(&out, &path)
                .await?;

            out.chown(&path, header.uid()? as u32, header.gid()? as u32)
                .await?;
            out.set_permissions(&path, MemPermissions::from_mode(header.mode()?))
                .await?;

            debug!("read archive entry");
            let mut data = vec![];
            entry.read_to_end(&mut data).await?;
            tokio::io::copy(&mut data.as_slice(), &mut handle).await?;
        } else if header.entry_type().is_symlink() {
            let to = PathBuf::from(OsString::from_vec(
                header.link_name_bytes().as_ref().unwrap().to_vec(),
            ));
            debug!("read symlink: {} -> {}", path.display(), to.display());
            let path = if !path.starts_with("/") {
                PathBuf::from("/").join(path)
            } else {
                path
            };
            // SAFETY: These just need to try to create. We don't actually care
            // about any errors, since this is a memfs. The only error that
            // might come up seems to be a spurious "file already exists" error
            // when the parent dir is a symlink.
            #[allow(unused_must_use)]
            if let Some(parent) = path.parent() {
                debug!("creating parent(s): {}", parent.display());
                out.create_dir_all(parent).await;
            }
            debug!("creating the symlink!");
            debug!(
                "to ({}) exists: {}",
                to.display(),
                out.metadata(&to).await.is_ok()
            );
            debug!(
                "path ({}) exists: {}",
                path.display(),
                out.metadata(&path).await.is_ok()
            );
            out.symlink(to, path).await?;
        } else if header.entry_type().is_hard_link() {
            // If the file is a hardlink, just duplicate the file
            // TODO: Figure out how to make hardlinks not be hellfuck misery
            let to = PathBuf::from(OsString::from_vec(
                header.link_name_bytes().as_ref().unwrap().to_vec(),
            ));
            debug!("read hardlink: {} -> {}", path.display(), to.display());
            let path = if !path.starts_with("/") {
                PathBuf::from("/").join(path)
            } else {
                path
            };
            out.copy(to, path).await?;
        }
    }

    debug!("done reading entries!");

    Ok(TarInternalMetadata {
        delegate: out,
        compression: c,
        ordered_paths,
    })
}

async fn tar_close(
    disk: &MemFloppyDisk,
    scope: &Path,
    compression: CompressionType,
    ordered_paths: &IndexSet<PathBuf>,
) -> Result<()> {
    debug!("closing tar at {}", scope.display());
    let buffer = vec![];
    let mut file = tokio::fs::OpenOptions::new()
        .truncate(true)
        .write(true)
        .open(scope)
        .await?;
    let mut archive = tokio_tar_up2date::Builder::new(buffer);

    for path in ordered_paths {
        debug!("processing output archive path {}", path.display());

        if path.as_os_str() == "/" {
            debug!("not writing /!");
            continue;
        }

        let mut header = tokio_tar_up2date::Header::new_ustar();
        trace!("ustar header!");
        {
            let path = if path.starts_with("/") {
                path.strip_prefix("/").unwrap()
            } else {
                path
            };
            header.set_path(path)?;
        }
        let path = if !path.starts_with("/") {
            PathBuf::from("/").join(path)
        } else {
            path.to_path_buf()
        };
        let path = path.as_path();
        let kind = determine_file_type(disk, path).await?;
        trace!("set path!");

        if kind == EntryType::Regular {
            debug!("creating file: {}", path.display(),);
            let metadata = disk.metadata(path).await?;

            trace!("basic metadata");
            header.set_entry_type(EntryType::Regular);
            header.set_size(metadata.len());
            header.set_mode(metadata.permissions().mode());
            header.set_gid(metadata.gid()?.into());
            header.set_uid(metadata.uid()?.into());

            trace!("opening handle!");
            let mut handle = MemOpenOptions::new().read(true).open(disk, path).await?;

            trace!("checksum!");
            header.set_cksum();

            trace!("append!");
            archive.append(&header, &mut handle).await?;
        } else if kind == EntryType::Directory {
            debug!("creating dir: {}", path.display());
            let metadata = disk.metadata(path).await?;

            trace!("basic metadata");
            header.set_entry_type(EntryType::Directory);
            header.set_size(0);
            header.set_mode(metadata.permissions().mode());
            header.set_gid(metadata.gid()?.into());
            header.set_uid(metadata.uid()?.into());

            trace!("checksum");
            header.set_cksum();
            let empty: &[u8] = &[];
            trace!("append");
            archive.append(&header, empty).await?;
        } else if kind == EntryType::Symlink {
            let link = disk.read_link(path).await?;
            debug!("creating symlink: {} -> {}", path.display(), link.display());

            trace!("basic metadata");
            header.set_entry_type(EntryType::Symlink);
            header.set_link_name(link.to_str().unwrap())?;
            header.set_size(0);
            header.set_cksum();

            let empty: &[u8] = &[];
            trace!("append");
            archive.append(&header, empty).await?;
        }
    }

    let buffer = archive.into_inner().await?;
    smoosh::recompress(&mut buffer.as_slice(), &mut file, compression).await?;
    debug!("done writing archive!");

    Ok(())
}

async fn determine_file_type(disk: &MemFloppyDisk, path: &Path) -> Result<EntryType> {
    trace!("determine file type of: {}", path.display());
    match disk.read_link(path).await {
        Ok(_) => Ok(EntryType::Symlink),
        Err(read_link_err) => {
            trace!(
                "missing symlink metadata for {}: {}",
                path.display(),
                read_link_err
            );
            match disk.metadata(path).await {
                Ok(metadata) => {
                    let file_type = metadata.file_type();
                    if file_type.is_symlink() {
                        Ok(EntryType::Symlink)
                    } else if file_type.is_dir() {
                        Ok(EntryType::Directory)
                    } else if file_type.is_file() {
                        Ok(EntryType::Regular)
                    } else {
                        warn!("SUPER unknown file type (missing metadata + unknown file type '{:?}') for: {}", file_type, path.display());
                        Ok(EntryType::Regular)
                    }
                }
                Err(metadata_err) => {
                    warn!(
                        "unknown file type (missing metadata) for {}: {}",
                        path.display(),
                        metadata_err
                    );
                    Ok(EntryType::Regular)
                }
            }
        }
    }
}
