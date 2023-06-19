use std::io::Read;
use std::os::unix::prelude::{OsStrExt, OsStringExt};

use tracing::debug;

crate::util::archive_format!(Ar, "a.ar", ar_open, ar_close);

async fn ar_open<P: Into<PathBuf>>(path: P) -> Result<MemFloppyDisk> {
    let path = path.into();
    if !crate::util::exists_async(path.clone()).await {
        let _archive = ar::Archive::new(std::fs::File::create(path)?);
        return Ok(MemFloppyDisk::new());
    }

    debug!("opening ar file {}", path.display());
    let mut archive = ar::Archive::new(crate::util::sync_file(path)?);
    let out = MemFloppyDisk::new();

    while let Some(entry) = archive.next_entry() {
        let mut entry = entry?;
        let header = entry.header();
        let path = PathBuf::from(OsString::from_vec(header.identifier().to_vec()));
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
        entry.read_to_end(&mut data)?;
        tokio::io::copy(&mut data.as_slice(), &mut handle).await?;
        debug!("copied path!");
    }

    Ok(out)
}

async fn ar_close(disk: &MemFloppyDisk, scope: &Path) -> Result<()> {
    debug!("closing ar at {}", scope.display());
    let file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true) // TODO: Validate this is actually desired...
        .open(scope)?;
    let mut archive = ar::Builder::new(file);

    debug!("walking ar paths...");
    let paths = nyoom::walk(disk, Path::new("/")).await?;
    debug!("found {} paths!", paths.len());
    // We only need to write file paths into the ar.
    // Directories are implied by the file paths.
    for path in paths {
        debug!("processing archive path {}", path.display());
        let metadata = disk.metadata(&path).await?;
        if metadata.is_file() {
            debug!("reading from memfs...");
            let mut handle = MemOpenOptions::new().read(true).open(disk, &path).await?;

            let mut data = vec![];
            tokio::io::AsyncReadExt::read_to_end(&mut handle, &mut data).await?;
            debug!("read full file from memfs!");

            let mut header = ar::Header::new(
                path.strip_prefix("/")
                    .unwrap()
                    .as_os_str()
                    .as_bytes()
                    .to_vec(),
                data.len() as u64,
            );
            header.set_gid(metadata.gid()?);
            header.set_uid(metadata.uid()?);
            header.set_mode(metadata.permissions().mode());
            header.set_mtime(
                metadata
                    .modified()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            debug!("created header!");

            // TODO: async safety
            archive.append(&header, &mut data.as_slice())?;
            debug!("appended to archive!");
        }
    }

    debug!("finished closing ar!");
    Ok(())
}
