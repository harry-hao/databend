// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::sync::Arc;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_metrics::storage::metrics_inc_remote_io_read_bytes;
use databend_common_metrics::storage::metrics_inc_remote_io_seeks;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use opendal::Operator;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexError;
use vortex::error::VortexResult;
use vortex::io::file::CoalesceWindow;
use vortex::io::file::IntoReadSource;
use vortex::io::file::IoRequest;
use vortex::io::file::ReadSource;
use vortex::io::file::ReadSourceRef;
use vortex::io::runtime::Handle;

/// Coalescing window for I/O: merge reads within 1 MB into a single request, capped at 16 MB.
///
/// This matches the upstream `ObjectStoreReadSource` configuration and ensures Vortex's
/// `IoRequestStream` can batch nearby column-chunk reads rather than issuing one syscall per chunk.
const COALESCE_WINDOW: CoalesceWindow = CoalesceWindow {
    distance: 1 << 20,      // 1 MB gap
    max_size: 16 << 20,     // 16 MB max coalesced read
};

const CONCURRENCY: usize = 64;

/// OpenDAL-backed [`ReadSource`] that plugs into Vortex's `FileRead` + `IoRequestStream`
/// coalescing pipeline.
///
/// Using `IntoReadSource` (rather than the lower-level `VortexReadAt`) activates Vortex's
/// built-in coalescing engine: `FileSegmentSource` eagerly registers read futures before
/// polling them, and `IoRequestStream` batches requests within `COALESCE_WINDOW` into a
/// single range read. This mirrors how Parquet batches column I/O in Databend.
pub struct OpendalReadSource {
    op: Operator,
    location: String,
    uri: Arc<str>,
}

impl OpendalReadSource {
    pub fn new(op: Operator, location: &str) -> Self {
        Self {
            op,
            uri: Arc::from(location),
            location: location.to_string(),
        }
    }
}

impl IntoReadSource for OpendalReadSource {
    fn into_read_source(self, _handle: Handle) -> VortexResult<ReadSourceRef> {
        Ok(Arc::new(self))
    }
}

impl ReadSource for OpendalReadSource {
    fn uri(&self) -> &Arc<str> {
        &self.uri
    }

    fn coalesce_window(&self) -> Option<CoalesceWindow> {
        Some(COALESCE_WINDOW)
    }

    /// File size is fetched lazily via `stat`. When a cached footer is supplied,
    /// `open_read_at` skips `read_footer` and never calls `size()`, so the stat is
    /// avoided entirely on cache-hit paths.
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let op = self.op.clone();
        let location = self.location.clone();
        async move {
            op.stat(&location)
                .await
                .map(|meta| meta.content_length())
                .map_err(|e| {
                    VortexError::from(io::Error::other(format!(
                        "opendal stat({location}) failed: {e}"
                    )))
                })
        }
        .boxed()
    }

    fn drive_send(
        self: Arc<Self>,
        requests: BoxStream<'static, IoRequest>,
    ) -> BoxFuture<'static, ()> {
        let op = self.op.clone();
        let location = self.location.clone();
        async move {
            // Open the reader once; all coalesced requests in this scan share it.
            let reader = match op.reader(&location).await {
                Ok(r) => r,
                Err(open_err) => {
                    let msg = Arc::new(format!(
                        "opendal reader({location}) failed: {open_err}"
                    ));
                    requests
                        .for_each(move |req| {
                            let msg = msg.clone();
                            async move {
                                req.resolve(Err(VortexError::from(io::Error::other(
                                    msg.as_str(),
                                ))));
                            }
                        })
                        .await;
                    return;
                }
            };

            requests
                .map(move |req| {
                    let reader = reader.clone();
                    let range = req.range();
                    let alignment = req.alignment();
                    let len = req.len();
                    async move {
                        let result = reader
                            .read(range.clone())
                            .await
                            .map_err(|e| {
                                VortexError::from(io::Error::other(format!(
                                    "opendal read({range:?}) failed: {e}"
                                )))
                            })
                            .and_then(|data| {
                                let bytes = data.to_bytes();
                                if bytes.len() != len {
                                    return Err(VortexError::from(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        format!(
                                            "expected {len} bytes for range {range:?}, got {}",
                                            bytes.len()
                                        ),
                                    )));
                                }
                                if len > 0 {
                                    // Record one seek and `len` bytes per coalesced I/O call.
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::ScanBytesFromRemote,
                                        len,
                                    );
                                    metrics_inc_remote_io_read_bytes(len as u64);
                                    metrics_inc_remote_io_seeks(1);
                                }
                                Ok(ByteBuffer::from(bytes.to_vec()).aligned(alignment))
                            });
                        req.resolve(result);
                    }
                })
                .buffer_unordered(CONCURRENCY)
                .collect::<()>()
                .await
        }
        .boxed()
    }
}
