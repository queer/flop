use std::sync::Arc;

use smoosh::CompressionType;
use tokio::io::AsyncReadExt;
use tracing::debug;

crate::util::archive_format!(Cpio, "a.cpio", cpio_open, cpio_close);

async fn cpio_open<P: Into<PathBuf>>(path: P) -> Result<MemFloppyDisk> {
    let path = path.into();
    if !crate::util::exists_async(path.clone()).await {
        return Ok(MemFloppyDisk::new());
    }

    debug!("loading cpio archive from {}...", path.display());
    let mut file = crate::util::async_file(path).await?;
    let out = MemFloppyDisk::new();
    let mut data = vec![];
    file.read_to_end(&mut data).await?;
    debug!("loaded cpio archive!");

    debug!("reading cpio entries...");
    for file in cpio_reader::iter_files(&data) {
        debug!("reading next entry...");
        let file_path = if file.name().starts_with('/') {
            PathBuf::from(file.name())
        } else {
            PathBuf::from("/").join(file.name())
        };

        if let Some(parent) = file_path.parent() {
            out.create_dir_all(parent).await?;
        }
        let mut mem_file = MemOpenOptions::new()
            .create(true)
            .write(true)
            .open(&out, &file_path)
            .await?;
        debug!("found cpio file: {}", file_path.display());
        let cpio_file_content = file.file().to_vec();
        let mut buf = vec![];
        smoosh::recompress(
            &mut cpio_file_content.as_slice(),
            &mut buf,
            CompressionType::None,
        )
        .await?;

        // TODO: random trailing null bytes show up and idk WHY ;-;
        let mut buf_slice = if buf[buf.len() - 1] == 0 {
            &buf[0..buf.len() - 1]
        } else {
            buf.as_slice()
        };
        tokio::io::copy(&mut buf_slice, &mut mem_file).await?;
        debug!("copied bytes!");
        mem_file
            .set_permissions(MemPermissions::from_mode(file.mode().bits()))
            .await?;
        debug!("set perms!");

        let uid = file.uid();
        let gid = file.gid();
        out.chown(&file_path, uid, gid).await?;
        debug!("loaded file!");
    }

    Ok(out)
}

async fn cpio_close(disk: &MemFloppyDisk, scope: &Path) -> Result<()> {
    let scope_clone = scope.to_path_buf();
    debug!("closing cpio archive at {}...", scope.display());
    let archive = tokio::task::spawn_blocking(move || {
        Arc::new(std::sync::Mutex::new(
            std::fs::OpenOptions::new()
                .truncate(true)
                .write(true)
                .open(scope_clone)
                .unwrap(),
        ))
    })
    .await?;

    let paths = nyoom::walk(disk, Path::new("/")).await?;
    debug!("found {} paths!", paths.len());
    for path in paths {
        let metadata = disk.metadata(&path).await?;
        if metadata.is_file() {
            let writer = cpio::newc::Builder::new(&path.to_string_lossy());
            let mut handle = MemOpenOptions::new().read(true).open(disk, &path).await?;

            let mut data = vec![];
            handle.read_to_end(&mut data).await?;

            let archive = archive.clone();
            tokio::task::spawn_blocking(move || {
                let mut archive = archive.lock().unwrap();
                let mut writer = writer
                    .gid(metadata.gid().unwrap())
                    .uid(metadata.uid().unwrap())
                    .mode(metadata.permissions().mode())
                    .write(&mut *archive, metadata.len() as u32);

                std::io::copy(&mut data.as_slice(), &mut writer).unwrap();

                writer.finish().unwrap();
            })
            .await?;
            debug!("wrote file: {}", path.display());
        }
    }

    Ok(())
}
