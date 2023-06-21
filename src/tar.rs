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
    debug!("considering {}..", path.display());
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
            if let Some(parent) = to.parent() {
                debug!("creating parent(s): {}", parent.display());
                out.create_dir_all(parent).await?;
            }
            if let Some(parent) = path.parent() {
                debug!("creating parent(s): {}", parent.display());
                out.create_dir_all(parent).await?;
            }
            out.symlink(to, path).await?;
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

        let path = if path.starts_with("/") {
            path.strip_prefix("/").unwrap()
        } else {
            path
        };

        let mut header = tokio_tar_up2date::Header::new_ustar();
        header.set_path(path)?;

        let kind = determine_file_type(disk, path).await?;
        if kind == EntryType::Regular {
            debug!("creating file: {}", path.display());
            let metadata = disk.metadata(path).await?;

            header.set_entry_type(EntryType::Regular);
            header.set_size(metadata.len());
            header.set_mode(metadata.permissions().mode());
            header.set_gid(metadata.gid()?.into());
            header.set_uid(metadata.uid()?.into());

            let mut handle = MemOpenOptions::new().read(true).open(disk, path).await?;

            header.set_cksum();

            archive.append(&header, &mut handle).await?;
        } else if kind == EntryType::Directory {
            debug!("creating dir: {}", path.display());
            let metadata = disk.metadata(path).await?;

            header.set_entry_type(EntryType::Directory);
            header.set_size(0);
            header.set_mode(metadata.permissions().mode());
            header.set_gid(metadata.gid()?.into());
            header.set_uid(metadata.uid()?.into());

            header.set_cksum();
            let empty: &[u8] = &[];
            archive.append(&header, empty).await?;
        } else if kind == EntryType::Symlink {
            let link = disk.read_link(path).await?;
            debug!("creating symlink: {} -> {}", path.display(), link.display());

            header.set_entry_type(EntryType::Symlink);
            header.set_link_name(link.to_str().unwrap())?;
            header.set_size(0);
            header.set_cksum();

            let empty: &[u8] = &[];
            archive.append(&header, empty).await?;
        }
    }

    let buffer = archive.into_inner().await?;
    smoosh::recompress(&mut buffer.as_slice(), &mut file, compression).await?;
    debug!("done writing archive!");

    Ok(())
}

async fn determine_file_type(disk: &MemFloppyDisk, path: &Path) -> Result<EntryType> {
    match disk.read_link(path).await {
        Ok(_) => Ok(EntryType::Symlink),
        Err(_) => {
            let file_type = disk.metadata(path).await?.file_type();
            if file_type.is_symlink() {
                Ok(EntryType::Symlink)
            } else if file_type.is_dir() {
                Ok(EntryType::Directory)
            } else if file_type.is_file() {
                Ok(EntryType::Regular)
            } else {
                warn!("unknown file type for: {}", path.display());
                Ok(EntryType::Regular)
            }
        }
    }
}
