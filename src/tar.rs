use std::os::unix::prelude::OsStringExt;

use futures::TryStreamExt;
use tokio::io::AsyncReadExt;
use tracing::debug;

crate::util::archive_format!(Tar, "a.tar", tar_open, tar_close);

async fn tar_open<P: Into<PathBuf>>(path: P) -> Result<MemFloppyDisk> {
    let path = path.into();
    debug!("opening tar file {}", path.display());
    let mut archive = tokio_tar_up2date::Archive::new(tokio::fs::File::open(path).await?);
    let out = MemFloppyDisk::new();

    let mut entries = archive.entries()?;
    while let Some(mut entry) = entries.try_next().await? {
        let header = entry.header();
        let path = PathBuf::from(OsString::from_vec(header.path_bytes().as_ref().to_vec()));
        debug!("processing archive path {}", path.display());

        if let Some(parent) = path.parent() {
            out.create_dir_all(parent).await?;
        }
        let mut handle = MemOpenOptions::new()
            .create(true)
            .write(true)
            .open(&out, &path)
            .await?;

        let mut data = vec![];
        entry.read_to_end(&mut data).await?;
        tokio::io::copy(&mut data.as_slice(), &mut handle).await?;
        debug!("copied path!");
    }

    Ok(out)
}

async fn tar_close(disk: &MemFloppyDisk, scope: &Path) -> Result<()> {
    debug!("closing tar at {}", scope.display());
    let mut archive = tokio_tar_up2date::Builder::new(
        tokio::fs::OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(scope)
            .await?,
    );

    let paths = nyoom::walk(disk, Path::new("/")).await?;
    for path in paths {
        debug!("processing archive path {}", path.display());
        let metadata = disk.metadata(&path).await?;
        if metadata.is_file() {
            let mut handle = MemOpenOptions::new().read(true).open(disk, &path).await?;

            let mut data = vec![];
            handle.read_to_end(&mut data).await?;

            let mut header = tokio_tar_up2date::Header::new_ustar();
            header.set_path(path.strip_prefix("/").unwrap())?;
            header.set_size(data.len() as u64);
            header.set_gid(metadata.gid()?.into());
            header.set_uid(metadata.uid()?.into());
            header.set_mode(metadata.permissions().mode());
            header.set_cksum();

            archive.append(&header, &mut data.as_slice()).await?;
        }
    }

    Ok(())
}
