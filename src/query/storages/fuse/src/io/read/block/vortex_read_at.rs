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

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_metrics::storage::metrics_inc_remote_io_read_bytes;
use databend_common_metrics::storage::metrics_inc_remote_io_seeks;
use futures::FutureExt;
use futures::future::BoxFuture;
use opendal::Operator;
use opendal::Reader;
use vortex::buffer::Alignment;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::io::PerformanceHint;
use vortex::io::VortexReadAt;

/// Minimal OpenDAL-backed `VortexReadAt` implementation.
///
/// This is intentionally small and self-contained so we can switch to `vortex_opendal`
/// once dependency wiring is finalized.
#[derive(Clone)]
pub struct OpendalReadAt {
    reader: Reader,
    size: u64,
    performance_hint: PerformanceHint,
}

impl OpendalReadAt {
    pub async fn open(op: Operator, path: &str) -> VortexResult<Self> {
        // We stat for size; open_read_at relies on accurate bounds checking.
        let meta = op
            .stat(path)
            .await
            .map_err(|err| io::Error::other(format!("opendal stat({path}) failed: {err}")))?;

        // Use reader_with so callers can later tune chunk/gap/concurrency.
        let reader = op
            .reader(path)
            .await
            .map_err(|err| io::Error::other(format!("opendal reader({path}) failed: {err}")))?;

        Ok(Self {
            reader,
            size: meta.content_length(),
            performance_hint: PerformanceHint::object_storage(),
            // performance_hint: PerformanceHint::local(),
        })
    }
}

impl VortexReadAt for OpendalReadAt {
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let size = self.size;
        async move { Ok(size) }.boxed()
    }

    fn performance_hint(&self) -> PerformanceHint {
        self.performance_hint.clone()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<ByteBuffer>> {
        let reader = self.reader.clone();
        let size = self.size;
        async move {
            let end = offset.checked_add(length as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "read range overflows u64")
            })?;

            if end > size {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("requested range {offset}..{end} exceeds source size {size}"),
                )
                .into());
            }

            let data = reader.read(offset..end).await.map_err(|err| {
                io::Error::other(format!("opendal read({offset}..{end}) failed: {err}"))
            })?;

            let bytes = data.to_bytes();
            if bytes.len() != length {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "expected {length} bytes for range {offset}..{end}, got {}",
                        bytes.len()
                    ),
                )
                .into());
            }

            let len = bytes.len();
            if len > 0 {
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytesFromRemote,
                    len,
                );
                metrics_inc_remote_io_read_bytes(len as u64);
                metrics_inc_remote_io_seeks(1);
            }

            Ok(ByteBuffer::from(bytes.to_vec()).aligned(alignment))
        }
        .boxed()
    }
}
