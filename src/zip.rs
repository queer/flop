use std::io::Cursor;
use std::os::unix::prelude::OsStringExt;

use async_zip::tokio::read::seek::ZipFileReader;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{ZipDateTime, ZipEntryBuilder, ZipString};
use chrono::DateTime;
use smoosh::CompressionType;
use tokio::io::AsyncReadExt;
use tracing::debug;

crate::util::archive_format!(Zip, "a.zip", zip_open, zip_close);

async fn zip_open<P: Into<PathBuf>>(path: P) -> Result<(MemFloppyDisk, CompressionType)> {
    let path = path.into();
    if !crate::util::exists_async(path.clone()).await {
        let _archive = ZipFileWriter::with_tokio(tokio::fs::File::create(path).await?);
        return Ok((MemFloppyDisk::new(), CompressionType::None));
    }

    debug!("opening zip file {}", path.display());
    let mut file = crate::util::async_file(path).await?;
    let mut buffer = vec![];
    let c = smoosh::recompress(&mut file, &mut buffer, smoosh::CompressionType::None).await?;
    let mut archive = ZipFileReader::with_tokio(Cursor::new(buffer))
        .await
        .map_err(fix_err)?;
    let out = MemFloppyDisk::new();

    let archive_file = archive.file();
    let entries = archive_file.entries();
    for idx in 0..entries.len() {
        let mut zip_entry = archive.reader_with_entry(idx).await.map_err(fix_err)?;
        let entry = zip_entry.entry();
        let path = PathBuf::from(OsString::from_vec(entry.filename().as_bytes().to_vec()));
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
        zip_entry
            .read_to_end_checked(&mut data)
            .await
            .map_err(fix_err)?;
        tokio::io::copy(&mut data.as_slice(), &mut handle).await?;
        debug!("copied path!");
    }

    Ok((out, c))
}

async fn zip_close(disk: &MemFloppyDisk, scope: &Path, compression: CompressionType) -> Result<()> {
    debug!("closing zip at {}", scope.display());
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(scope)
        .await?;

    let buffer = vec![];
    let mut writer = ZipFileWriter::with_tokio(buffer);

    let paths = nyoom::walk_ordered(disk, Path::new("/")).await?;
    for path in paths {
        let metadata = disk.metadata(&path).await?;
        if metadata.is_file() {
            debug!("writing path {} to zip!", path.display());
            let mut handle = MemOpenOptions::new().read(true).open(disk, &path).await?;

            let mut data = vec![];
            handle.read_to_end(&mut data).await?;

            // TODO: lol not utf8
            let entry = ZipEntryBuilder::new(
                ZipString::new(
                    path.to_string_lossy().to_string().as_bytes().to_vec(),
                    async_zip::StringEncoding::Utf8,
                ),
                async_zip::Compression::Stored,
            );

            let entry = entry
                .last_modification_date(ZipDateTime::from_chrono(&DateTime::from(
                    metadata.modified().unwrap(),
                )))
                .unix_permissions(metadata.permissions().mode() as u16);

            writer
                .write_entry_whole(entry, &data)
                .await
                .map_err(fix_err)?;

            debug!("wrote path!");
        }
    }

    let writer = writer.close().await.map_err(fix_err)?;
    smoosh::recompress(&mut writer.get_ref().as_slice(), &mut file, compression).await?;

    Ok(())
}

fn fix_err<E: std::error::Error + Send + Sync + 'static>(err: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, err)
}
