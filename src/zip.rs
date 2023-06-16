use std::os::unix::prelude::OsStringExt;

use async_zip::tokio::read::seek::ZipFileReader;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{ZipDateTime, ZipEntryBuilder, ZipString};
use chrono::DateTime;
use tokio::io::AsyncReadExt;
use tracing::debug;

crate::util::archive_format!(Zip, "a.zip", zip_open, zip_close);

async fn zip_open<P: Into<PathBuf>>(path: P) -> Result<MemFloppyDisk> {
    let path = path.into();
    debug!("opening zip file {}", path.display());
    let mut archive = ZipFileReader::with_tokio(tokio::fs::File::open(path).await?)
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

    Ok(out)
}

async fn zip_close(disk: &MemFloppyDisk, scope: &Path) -> Result<()> {
    debug!("closing zip at {}", scope.display());
    let out = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(scope)
        .await?;

    let mut writer = ZipFileWriter::with_tokio(out);

    let paths = nyoom::walk(disk, Path::new("/")).await?;
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

    writer.close().await.map_err(fix_err)?;

    Ok(())
}

fn fix_err<E: std::error::Error + Send + Sync + 'static>(err: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, err)
}
