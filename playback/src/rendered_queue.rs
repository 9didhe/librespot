use std::{collections::VecDeque, sync::Arc};

use crate::listening::{RenderedAudio, RenderedChunk};

/// An output's submitted frames, in playback order, across track changes.
/// The queue contains clocks, never audio packets or network reports.
pub(crate) struct RenderedQueue {
    sample_rate: u32,
    submitted: u64,
    consumed: u64,
    chunks: VecDeque<QueuedChunk>,
}

struct QueuedChunk {
    remaining: u64,
    clock: Option<RenderedChunk>,
}

impl RenderedQueue {
    pub fn new(sample_rate: u32) -> Self {
        Self {
            sample_rate,
            submitted: 0,
            consumed: 0,
            chunks: VecDeque::new(),
        }
    }

    pub fn submitted(&self) -> u64 {
        self.submitted
    }

    pub fn pending(&self) -> bool {
        !self.chunks.is_empty()
    }

    pub fn submit(&mut self, frames: u64, audio: Option<&Arc<RenderedAudio>>) {
        if frames == 0 {
            return;
        }
        self.submitted += frames;
        self.chunks.push_back(QueuedChunk {
            remaining: frames,
            clock: audio.map(|audio| audio.chunk(self.sample_rate)),
        });
    }

    /// Snapshot submitted frames before asking the server for latency. New
    /// writes may race the query: subtracting latency from a later submission
    /// count would incorrectly include new, unplayed frames.
    pub fn observe(&mut self, submitted_before_query: u64, latency_us: u64) {
        // Round queued frames up, so fractional-frame latency never counts a
        // frame whose playback has not yet been confirmed by the server clock.
        let queued =
            (u128::from(latency_us) * u128::from(self.sample_rate)).div_ceil(1_000_000) as u64;
        self.advance(submitted_before_query.saturating_sub(queued));
    }

    pub fn drained(&mut self) {
        self.advance(self.submitted);
    }

    pub fn discard(&mut self) {
        // Dropping a partially consumed guard publishes only its known frames.
        self.chunks.clear();
        self.consumed = self.submitted;
    }

    fn advance(&mut self, consumed: u64) {
        let consumed = consumed.min(self.submitted).max(self.consumed);
        let mut frames = consumed - self.consumed;
        while frames != 0 {
            let chunk = self
                .chunks
                .front_mut()
                .expect("submitted frames have a chunk");
            let rendered = frames.min(chunk.remaining);
            if let Some(clock) = &mut chunk.clock {
                clock.rendered(rendered);
            }
            chunk.remaining -= rendered;
            frames -= rendered;
            if chunk.remaining == 0 {
                self.chunks.pop_front();
            }
        }
        self.consumed = consumed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn partial_frames_and_failed_tails_count_only_confirmed_playback() {
        let audio = RenderedAudio::new();
        let mut queue = RenderedQueue::new(44_100);
        queue.submit(44_100, Some(&audio));
        // Half a second plus one microsecond still has 22,051 queued frames.
        queue.observe(queue.submitted(), 500_001);
        assert!(queue.pending());
        queue.discard();
        assert_eq!(
            audio.played(),
            Duration::from_nanos(22_049 * 1_000_000_000 / 44_100)
        );
    }

    #[test]
    fn writes_racing_a_latency_query_cannot_count_unplayed_frames() {
        let audio = RenderedAudio::new();
        let mut queue = RenderedQueue::new(1_000);
        queue.submit(100, Some(&audio));
        let before_query = queue.submitted();
        // This write completes after the latency observation has been made.
        queue.submit(100, Some(&audio));
        queue.observe(before_query, 100_000);
        queue.discard();
        assert_eq!(audio.played(), Duration::ZERO);
        assert!(audio.started_at().is_none());
    }

    #[test]
    fn gapless_track_clocks_settle_in_order_without_counting_the_next_track() {
        let first = RenderedAudio::new();
        let second = RenderedAudio::new();
        let mut queue = RenderedQueue::new(48_000);
        queue.submit(48_000, Some(&first));
        queue.submit(48_000, Some(&second));
        queue.observe(queue.submitted(), 1_000_000);
        assert_eq!(first.played(), Duration::from_secs(1));
        assert_eq!(second.played(), Duration::ZERO);
        assert!(queue.pending());
        // Another observation settles an EOF tail, with no further write or stop.
        queue.observe(queue.submitted(), 0);
        assert_eq!(second.played(), Duration::from_secs(1));
        assert!(!queue.pending());
    }

    #[test]
    fn a_successful_drain_confirms_all_frames_and_preserves_the_pause_clock() {
        let audio = RenderedAudio::new();
        let mut before_pause = RenderedQueue::new(44_100);
        before_pause.submit(44_100, Some(&audio));
        before_pause.drained();
        let mut after_pause = RenderedQueue::new(44_100);
        after_pause.submit(88_200, Some(&audio));
        after_pause.drained();
        assert_eq!(audio.played(), Duration::from_secs(3));
        assert!(!before_pause.pending());
        assert!(!after_pause.pending());
    }

    #[test]
    fn latency_jitter_cannot_undo_confirmed_frames() {
        let audio = RenderedAudio::new();
        let mut queue = RenderedQueue::new(1_000);
        queue.submit(1_000, Some(&audio));
        queue.observe(queue.submitted(), 500_000);
        queue.observe(queue.submitted(), 700_000);
        queue.discard();
        assert_eq!(audio.played(), Duration::from_millis(500));
    }

    #[test]
    fn untracked_audio_keeps_following_tracks_on_the_same_output_timeline() {
        let audio = RenderedAudio::new();
        let mut queue = RenderedQueue::new(1_000);
        queue.submit(1_000, None);
        queue.submit(1_000, Some(&audio));
        queue.observe(queue.submitted(), 1_000_000);
        assert_eq!(audio.played(), Duration::ZERO);
        queue.drained();
        assert_eq!(audio.played(), Duration::from_secs(1));
    }

    #[test]
    fn dropping_the_output_does_not_count_its_queued_tail() {
        let audio = RenderedAudio::new();
        let mut queue = RenderedQueue::new(1_000);
        queue.submit(1_000, Some(&audio));
        queue.observe(queue.submitted(), 250_000);
        drop(queue);
        assert_eq!(audio.played(), Duration::from_millis(750));
    }
}
