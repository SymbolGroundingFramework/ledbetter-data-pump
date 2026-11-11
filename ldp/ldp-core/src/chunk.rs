//! The unit of data in flight (contract §3.1).

use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
pub struct DataChunk {
    pub chunk_id: u64,
    pub batch: RecordBatch,
}

impl DataChunk {
    pub fn new(chunk_id: u64, batch: RecordBatch) -> Self {
        Self { chunk_id, batch }
    }

    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}
