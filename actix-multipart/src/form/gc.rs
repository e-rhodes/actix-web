//! Uploads a file to Google Cloud Storage.
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::Infallible;
use std::marker::PhantomData;

use crate::form::{config_from_req, FieldReader, Limits};
use crate::{Field, MultipartError};
use actix_web::error::{
    ErrorBadRequest, ErrorInternalServerError, ErrorUnprocessableEntity,
    ErrorUnsupportedMediaType,
};
use actix_web::rt::task::yield_now;
use actix_web::HttpRequest;
use futures_core::future::LocalBoxFuture;
use futures_util::{StreamExt, TryFutureExt, TryStreamExt};
pub use googlecloud_dep::{Client, Object};
use tokio_stream::wrappers::ReceiverStream;
// use tracing::{debug, trace};

/// Result from an upload to Google Cloud.
pub struct GoogleCloudUpload {
    /// The object that was uploaded to the specified location.
    pub object: Object,
}

impl GoogleCloudUpload {
    fn new(object: Object) -> Self {
        Self { object }
    }
    fn into_inner(self) -> Object {
        self.object
    }
}

impl<'t> FieldReader<'t> for GoogleCloudUpload {
    type Future = LocalBoxFuture<'t, Result<Self, MultipartError>>;

    fn read_field(
        req: &'t HttpRequest,
        mut field: Field,
        limits: &'t mut Limits,
    ) -> Self::Future {
        let name = field.name().to_string();
        let fut = async move {
            let config = GoogleCloudUploadConfig::from_req(req);
            let client = config.client();
            let bucket = config.bucket(&name);
            let length = field
                .headers()
                .get("Content-Length")
                .map(|header_val| {
                    header_val
                        .to_str()
                        .map_err(ErrorBadRequest) // if it exists, it should be just digits
                        .and_then(|val| val.parse::<u64>().map_err(ErrorBadRequest))
                })
                .transpose()?;
            let filename = field
                .content_disposition()
                .get_filename()
                .ok_or(ErrorUnprocessableEntity("No filename found in header!"))?;
            let path = config.path(&name, filename).to_owned();

            let mime_type = field
                .content_type()
                .cloned()
                .unwrap_or(mime::APPLICATION_OCTET_STREAM);
            if mime_type != mime::APPLICATION_PDF {
                return Err(ErrorUnsupportedMediaType(format!(
                    "Expected pdf, got {mime_type}"
                )));
            }
            let (mut tx, rx) = tokio::sync::mpsc::channel(4);
            let rx = ReceiverStream::new(rx);
            let upload_fut = async move {
                // debug!("Starting upload to {path_clone}");
                #[allow(unused_qualifications)] // typing the error, false positive
                let stream = rx
                    .then(|chunk| async {
                        yield_now().await;
                        chunk
                    })
                    .map(Result::<_, Infallible>::Ok);
                client
                    .object()
                    .create_streamed(&bucket, stream, length, &path, mime_type.as_ref())
                    .await
            };
            let upload_handle = actix_web::rt::spawn(upload_fut);

            while let Some(chunk) = field.try_next().await? {
                let len = chunk.len();
                limits.try_consume_limits(len, false)?;
                tx.send(chunk)
                    .await
                    .map_err(|_| ErrorInternalServerError("Stream closed unexpectedly!"))?;
            }
            drop(tx);

            let object = upload_handle
                .await
                .map_err(ErrorInternalServerError)?
                .map_err(ErrorInternalServerError)?;

            Ok(Self::new(object))
        }
        .map_err(|e| MultipartError::Field {
            field_name: field.name().to_string(),
            source: e,
        });
        Box::pin(fut)
    }
}

const DEFAULT_CONFIG: GoogleCloudUploadConfig = GoogleCloudUploadConfig {
    client: None,
    path: None,
    bucket: None,
}
#[derive(Default)]
pub struct GoogleCloudUploadConfig {
    client: Option<Client>,
    path: Option<Box<dyn Fn(&str, &str) -> String>>,
    bucket: Option<Box<dyn Fn(&str) -> String>>,
}
impl GoogleCloudUploadConfig {
    fn from_req(req: &HttpRequest) -> &Self {
        config_from_req(req, &DEFAULT_CONFIG)
    }
    fn client(&self) -> Option<&Client> {
        self.client.as_ref()
    }
    fn path<'a>(&self, path: &'a str, name: &str) -> Cow<'a, str> {
        self.path
            .as_ref()
            .map_or(Cow::Borrowed(path), |f| Cow::Owned(f(name, path)))
    }
    fn bucket(&self, name: &str) -> Cow<'_, str> {
        self.bucket
            .as_ref()
            .map_or(Cow::Borrowed("TODO"), |f| Cow::Owned(f(name)))
    }
}
