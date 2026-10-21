use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use librespot_core::{
    Error, FileId, Session, SpotifyUri,
    listening::{EndReason, ListeningReporter, PlaybackReport},
};
use librespot_metadata::audio::AudioFileFormat;
use tokio::sync::{mpsc, oneshot, watch};

/// An explicit finalization cancels work, while ordinary sender closure leaves
/// queued reports and loaders to finish with the player's existing semantics.
pub(crate) async fn until_finalized<T>(
    mut finalized: watch::Receiver<bool>,
    work: impl std::future::Future<Output = T>,
) -> Option<T> {
    let cancellation = async {
        loop {
            if *finalized.borrow() {
                return;
            }
            if finalized.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        biased;
        () = cancellation => None,
        result = work => Some(result),
    }
}

pub(crate) enum ReportCommand {
    Report(u64, Session, Box<PendingReport>),
    Flush(oneshot::Sender<Result<(), Error>>),
}

pub(crate) struct PendingFlush {
    pub sender: mpsc::Sender<ReportCommand>,
    pub final_report: Option<ReportCommand>,
    pub previous_error: Option<Error>,
}

impl PendingFlush {
    pub async fn flush(self) -> Result<(), Error> {
        // Wait for queue capacity on the caller's runtime, never the audio thread.
        if let Some(report) = self.final_report {
            self.sender.send(report).await?;
        }
        let (done, completed) = oneshot::channel();
        self.sender.send(ReportCommand::Flush(done)).await?;
        completed.await??;
        self.previous_error.map_or(Ok(()), Err)
    }
}

trait ReportSink {
    fn report(
        &mut self,
        generation: u64,
        session: Session,
        report: PlaybackReport,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
}

struct SessionReporter {
    generation: u64,
    reporter: ListeningReporter,
}

impl ReportSink for SessionReporter {
    async fn report(
        &mut self,
        generation: u64,
        session: Session,
        report: PlaybackReport,
    ) -> Result<(), Error> {
        if generation != self.generation {
            self.generation = generation;
            self.reporter = ListeningReporter::new(session);
        }
        tokio::time::timeout(Duration::from_secs(60), self.reporter.report(&report))
            .await
            .map_err(|_| Error::deadline_exceeded("Listening report timed out"))??;
        debug!(
            "Reported completed listening for {} ({} ms)",
            report.uri,
            report.played.as_millis()
        );
        Ok(())
    }
}

async fn run_reports(mut receiver: mpsc::Receiver<ReportCommand>, mut reporter: impl ReportSink) {
    let mut pending_error = None;
    while let Some(command) = receiver.recv().await {
        match command {
            ReportCommand::Report(generation, session, playback) => {
                // A decoder can finish while its buffered tail is still playing.
                // Waiting belongs here, never on the decoder or output thread.
                // A paused output may retain a source for longer than a network
                // deadline. Only transport has a deadline; application shutdown
                // bounds the complete flush externally.
                let Some(playback) = playback.finish().await else {
                    continue;
                };
                if let Err(error) = reporter.report(generation, session, playback).await {
                    warn!("Unable to report listening: {error}");
                    pending_error = Some(error);
                }
            }
            ReportCommand::Flush(done) => {
                let _ = done.send(pending_error.take().map_or(Ok(()), Err));
            }
        }
    }
}

/// Playback clock for a buffered output. Each queued source owns a chunk guard;
/// only consumed frames count, and dropping queued audio settles the guard
/// without counting the discarded remainder. Volume does not affect the clock.
#[derive(Default)]
pub struct RenderedAudio {
    outstanding: AtomicUsize,
    played_ns: AtomicU64,
    /// Milliseconds since the epoch, plus one so zero remains the unset value.
    started_ms: AtomicU64,
    ended_ms: AtomicU64,
}

impl RenderedAudio {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Capture this guard in the queued source, before handing it to the output.
    /// The rate is the source's output rate, after any resampling.
    pub fn chunk(self: &Arc<Self>, sample_rate: u32) -> RenderedChunk {
        self.outstanding.fetch_add(1, Ordering::Relaxed);
        RenderedChunk {
            audio: Arc::clone(self),
            sample_rate: sample_rate.max(1),
            frames: 0,
        }
    }

    /// The time of the first rendered frame, absent for entirely discarded audio.
    pub fn started_at(&self) -> Option<SystemTime> {
        self.started_ms
            .load(Ordering::Acquire)
            .checked_sub(1)
            .map(|ms| SystemTime::UNIX_EPOCH + Duration::from_millis(ms))
    }

    /// Rendered duration published by sources that have ended or been discarded.
    /// A still-active chunk publishes its frames when its guard is dropped.
    pub fn played(&self) -> Duration {
        Duration::from_nanos(self.played_ns.load(Ordering::Acquire))
    }

    fn ended_at(&self) -> Option<SystemTime> {
        self.ended_ms
            .load(Ordering::Acquire)
            .checked_sub(1)
            .map(|ms| SystemTime::UNIX_EPOCH + Duration::from_millis(ms))
    }

    async fn settled(&self) {
        // Polling keeps the output callback free of locks and async wakeups.
        while self.outstanding.load(Ordering::Acquire) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// One queued source's rendered frames. `rendered` only updates local state
/// after the first frame; duration is published atomically when the source ends.
pub struct RenderedChunk {
    audio: Arc<RenderedAudio>,
    sample_rate: u32,
    frames: u64,
}

impl RenderedChunk {
    /// Count frames actually supplied to the output, including zero-volume audio.
    /// Do not call this for frames removed by a seek, skip, or failed output.
    pub fn rendered(&mut self, frames: u64) {
        self.rendered_at(frames, SystemTime::now);
    }

    fn rendered_at(&mut self, frames: u64, now: impl FnOnce() -> SystemTime) {
        if frames == 0 {
            return;
        }
        if self.frames == 0 {
            let ms = now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX - 1)) as u64;
            let _ = self.audio.started_ms.compare_exchange(
                0,
                ms + 1,
                Ordering::Release,
                Ordering::Relaxed,
            );
        }
        self.frames = self.frames.saturating_add(frames);
    }
}

impl Drop for RenderedChunk {
    fn drop(&mut self) {
        let ns = (u128::from(self.frames) * 1_000_000_000 / u128::from(self.sample_rate))
            .min(u128::from(u64::MAX)) as u64;
        self.audio.played_ns.fetch_add(ns, Ordering::Relaxed);
        if self.frames != 0 {
            let ms = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX - 1)) as u64;
            self.audio.ended_ms.fetch_max(ms + 1, Ordering::Relaxed);
        }
        // Publish the duration before the final guard marks the track settled.
        self.audio.outstanding.fetch_sub(1, Ordering::Release);
    }
}

pub(crate) fn reporter(
    session: Session,
    finalized: watch::Receiver<bool>,
) -> mpsc::Sender<ReportCommand> {
    let (sender, receiver) = mpsc::channel(32);
    let reporter = SessionReporter {
        generation: 0,
        reporter: ListeningReporter::new(session.clone()),
    };
    session.spawn(until_finalized(finalized, run_reports(receiver, reporter)));
    sender
}

#[derive(Clone, Copy)]
pub(crate) struct LoadedAudioFile {
    pub id: FileId,
    pub format: AudioFileFormat,
}

pub(crate) struct PlaybackStatistics {
    uri: SpotifyUri,
    file: LoadedAudioFile,
    context_uri: Option<String>,
    started_at: Option<SystemTime>,
    samples: u64,
    rendered: Option<Arc<RenderedAudio>>,
}

impl PlaybackStatistics {
    pub fn new(uri: SpotifyUri, file: LoadedAudioFile, context_uri: Option<String>) -> Self {
        Self {
            uri,
            file,
            context_uri,
            started_at: None,
            samples: 0,
            rendered: None,
        }
    }

    pub fn restart(&self) -> Self {
        Self::new(self.uri.clone(), self.file, self.context_uri.clone())
    }

    pub fn use_rendered_audio(&mut self, audio: Arc<RenderedAudio>) {
        self.rendered = Some(audio);
    }

    pub fn configure_output(&mut self, sink: &mut dyn crate::audio_backend::Sink) {
        let audio = RenderedAudio::new();
        if sink.set_rendered_audio(Some(Arc::clone(&audio))) {
            self.use_rendered_audio(audio);
        }
    }

    pub fn pending(self, reason: EndReason, at: SystemTime) -> Option<PendingReport> {
        // A source can be queued before its first output callback. Keep it until
        // the guards settle, then decide whether any listening actually happened.
        if self.started_at.is_none()
            && self.rendered.as_ref().is_none_or(|audio| {
                audio.started_at().is_none() && audio.outstanding.load(Ordering::Acquire) == 0
            })
        {
            return None;
        }
        Some(PendingReport {
            statistics: self,
            reason,
            ended_at: at,
        })
    }

    pub fn written(&mut self, samples: usize, at: SystemTime) {
        if self.rendered.is_none() && samples > 0 {
            self.started_at.get_or_insert(at);
            self.samples = self.samples.saturating_add(samples as u64);
        }
    }

    pub fn finish(self, reason: EndReason, at: SystemTime) -> Option<PlaybackReport> {
        let (started_at, played) = match &self.rendered {
            Some(audio) => (audio.started_at()?, audio.played()),
            None => (
                self.started_at?,
                Duration::from_millis(
                    self.samples.saturating_mul(1000) / crate::SAMPLES_PER_SECOND as u64,
                ),
            ),
        };
        if played.is_zero() {
            return None;
        }
        let format = match self.file.format {
            AudioFileFormat::OGG_VORBIS_96 => "Vorbis 96 kbps",
            AudioFileFormat::OGG_VORBIS_160 => "Vorbis 160 kbps",
            AudioFileFormat::OGG_VORBIS_320 => "Vorbis 320 kbps",
            AudioFileFormat::MP3_96 => "MP3 96 kbps",
            AudioFileFormat::MP3_160 => "MP3 160 kbps",
            AudioFileFormat::MP3_256 => "MP3 256 kbps",
            AudioFileFormat::MP3_320 => "MP3 320 kbps",
            _ => return None,
        };
        Some(PlaybackReport {
            uri: self.uri,
            file_id: self.file.id,
            context_uri: self.context_uri,
            audio_format: format.into(),
            started_at,
            ended_at: at,
            played,
            reason,
        })
    }
}

pub(crate) struct PendingReport {
    statistics: PlaybackStatistics,
    reason: EndReason,
    ended_at: SystemTime,
}

impl PendingReport {
    async fn finish(self) -> Option<PlaybackReport> {
        let ended_at = if let Some(audio) = &self.statistics.rendered {
            audio.settled().await;
            self.ended_at.max(audio.ended_at().unwrap_or(self.ended_at))
        } else {
            self.ended_at
        };
        self.statistics.finish(self.reason, ended_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SAMPLES_PER_SECOND;

    #[tokio::test]
    async fn finalized_work_is_never_polled() {
        let (_, finalized) = watch::channel(true);
        let polled = Arc::new(AtomicUsize::new(0));
        let observed = polled.clone();
        let result = until_finalized(finalized, async move {
            polled.fetch_add(1, Ordering::SeqCst);
        })
        .await;
        assert_eq!(result, None);
        assert_eq!(observed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn finalization_cancels_a_pending_loader_without_delivering_its_result() {
        let (finalize, finalized) = watch::channel(false);
        let (held, released) = oneshot::channel::<()>();
        let loader = tokio::spawn(until_finalized(finalized, async move {
            let _held_while_loading = held;
            std::future::pending::<()>().await;
            "loaded track"
        }));
        tokio::task::yield_now().await;
        finalize.send_replace(true);
        assert!(loader.await.unwrap().is_none());
        assert!(released.await.is_err(), "loading future was dropped");
    }

    #[tokio::test]
    async fn ordinary_player_closure_keeps_queued_reports_until_completion() {
        let (finalize, finalized) = watch::channel(false);
        drop(finalize);
        let (sender, receiver) = mpsc::channel(4);
        let reports = Arc::new(AtomicUsize::new(0));
        struct CountingReporter(Arc<AtomicUsize>);
        impl ReportSink for CountingReporter {
            async fn report(&mut self, _: u64, _: Session, _: PlaybackReport) -> Result<(), Error> {
                tokio::task::yield_now().await;
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
        let mut statistics = loaded();
        statistics.written(SAMPLES_PER_SECOND as usize, SystemTime::now());
        sender
            .send(ReportCommand::Report(
                0,
                Session::new(Default::default(), None),
                Box::new(
                    statistics
                        .pending(EndReason::TrackDone, SystemTime::now())
                        .unwrap(),
                ),
            ))
            .await
            .unwrap();
        drop(sender);
        assert!(
            until_finalized(
                finalized,
                run_reports(receiver, CountingReporter(reports.clone()))
            )
            .await
            .is_some()
        );
        assert_eq!(reports.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn finalization_cancels_a_pending_report_and_closes_its_queue() {
        let (finalize, finalized) = watch::channel(false);
        let (sender, receiver) = mpsc::channel(4);
        let (started, reporting) = oneshot::channel();
        struct PendingReporter(Option<oneshot::Sender<()>>);
        impl ReportSink for PendingReporter {
            async fn report(&mut self, _: u64, _: Session, _: PlaybackReport) -> Result<(), Error> {
                self.0.take().unwrap().send(()).unwrap();
                std::future::pending().await
            }
        }
        let mut statistics = loaded();
        statistics.written(SAMPLES_PER_SECOND as usize, SystemTime::now());
        sender
            .send(ReportCommand::Report(
                0,
                Session::new(Default::default(), None),
                Box::new(
                    statistics
                        .pending(EndReason::EndPlay, SystemTime::now())
                        .unwrap(),
                ),
            ))
            .await
            .unwrap();
        let task = tokio::spawn(until_finalized(
            finalized,
            run_reports(receiver, PendingReporter(Some(started))),
        ));
        reporting.await.unwrap();
        finalize.send_replace(true);
        assert!(task.await.unwrap().is_none());
        assert!(sender.is_closed());
    }

    fn loaded() -> PlaybackStatistics {
        PlaybackStatistics::new(
            SpotifyUri::from_uri("spotify:track:72AZ3V52rs9NfgMNhALxln").unwrap(),
            LoadedAudioFile {
                id: FileId([1; 20]),
                format: AudioFileFormat::OGG_VORBIS_320,
            },
            None,
        )
    }

    fn buffered() -> (PlaybackStatistics, Arc<RenderedAudio>) {
        let mut stats = loaded();
        let audio = RenderedAudio::new();
        stats.use_rendered_audio(Arc::clone(&audio));
        (stats, audio)
    }

    #[test]
    fn a_sink_without_a_render_hook_keeps_accepted_sample_accounting() {
        struct SynchronousSink;
        impl crate::audio_backend::Sink for SynchronousSink {
            fn write(
                &mut self,
                _: crate::decoder::AudioPacket,
                _: &mut crate::convert::Converter,
            ) -> crate::audio_backend::SinkResult<()> {
                Ok(())
            }
        }
        let mut stats = loaded();
        stats.configure_output(&mut SynchronousSink);
        stats.written(SAMPLES_PER_SECOND as usize, SystemTime::now());
        let report = stats
            .finish(EndReason::TrackDone, SystemTime::now())
            .unwrap();
        assert_eq!(report.played, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn completed_listen_waits_for_the_rendered_buffer_tail() {
        let (stats, audio) = buffered();
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let mut beginning = audio.chunk(44_100);
        beginning.rendered_at(44_100, || start);
        drop(beginning);
        let mut tail = audio.chunk(48_000);
        let pending = stats.pending(EndReason::TrackDone, start).unwrap();
        let finish = pending.finish();
        tokio::pin!(finish);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut finish)
                .await
                .is_err()
        );
        tail.rendered_at(48_000, || start + Duration::from_secs(1));
        drop(tail);
        let report = finish.await.unwrap();
        assert_eq!(report.played, Duration::from_secs(2));
        assert_eq!(report.started_at, start);
        assert!(matches!(report.reason, EndReason::TrackDone));
    }

    #[tokio::test]
    async fn interrupted_listen_counts_only_the_rendered_part_of_discarded_sources() {
        let (mut stats, audio) = buffered();
        let mut partial = audio.chunk(48_000);
        let discarded = audio.chunk(48_000);
        partial.rendered(24_000);
        // Accepted decoder packets cannot double-count the custom output clock.
        stats.written(SAMPLES_PER_SECOND as usize * 20, SystemTime::now());
        let pending = stats
            .pending(EndReason::EndPlay, SystemTime::now())
            .unwrap();
        drop(partial);
        drop(discarded);
        let report = pending.finish().await.unwrap();
        assert_eq!(report.played, Duration::from_millis(500));
        assert!(matches!(report.reason, EndReason::EndPlay));
    }

    #[tokio::test]
    async fn audio_discarded_before_the_first_render_is_not_a_listen() {
        let (stats, audio) = buffered();
        let discarded = audio.chunk(44_100);
        let pending = stats
            .pending(EndReason::EndPlay, SystemTime::now())
            .unwrap();
        drop(discarded);
        assert!(pending.finish().await.is_none());
    }

    #[tokio::test]
    async fn pause_and_seek_keep_one_rendered_clock_without_counting_discarded_audio() {
        let (stats, audio) = buffered();
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let mut before_pause = audio.chunk(44_100);
        before_pause.rendered_at(44_100, || start);
        drop(before_pause);
        // A seek removes queued audio; a pause supplies no frames. Resuming
        // keeps this tracker, even when wall time or the seek cursor jumps.
        let seek_discard = audio.chunk(44_100);
        drop(seek_discard);
        let mut after_seek = audio.chunk(48_000);
        after_seek.rendered_at(96_000, || start + Duration::from_secs(60));
        drop(after_seek);
        let report = stats
            .pending(EndReason::TrackDone, start + Duration::from_secs(62))
            .unwrap()
            .finish()
            .await
            .unwrap();
        assert_eq!(report.started_at, start);
        assert_eq!(report.played, Duration::from_secs(3));
    }

    #[tokio::test]
    async fn a_stalled_render_clock_does_not_prevent_a_bounded_flush() {
        let (stats, audio) = buffered();
        let stalled = audio.chunk(44_100);
        let (sender, receiver) = mpsc::channel(4);
        struct UnexpectedReport;
        impl ReportSink for UnexpectedReport {
            async fn report(&mut self, _: u64, _: Session, _: PlaybackReport) -> Result<(), Error> {
                panic!("discarded audio must not be reported")
            }
        }
        let worker = tokio::spawn(run_reports(receiver, UnexpectedReport));
        let pending = PendingFlush {
            sender: sender.clone(),
            final_report: Some(ReportCommand::Report(
                0,
                Session::new(Default::default(), None),
                Box::new(
                    stats
                        .pending(EndReason::EndPlay, SystemTime::now())
                        .unwrap(),
                ),
            )),
            previous_error: None,
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(10), pending.flush())
                .await
                .is_err()
        );
        drop(stalled);
        drop(sender);
        worker.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_long_pause_does_not_expire_a_buffered_listen() {
        let (stats, audio) = buffered();
        let mut paused = audio.chunk(44_100);
        let (sender, receiver) = mpsc::channel(4);
        let (record, recorded) = oneshot::channel();
        let (done, mut completed) = oneshot::channel();
        struct RecordedReport(Option<oneshot::Sender<PlaybackReport>>);
        impl ReportSink for RecordedReport {
            async fn report(
                &mut self,
                _: u64,
                _: Session,
                playback: PlaybackReport,
            ) -> Result<(), Error> {
                self.0.take().unwrap().send(playback).unwrap();
                Ok(())
            }
        }
        let worker = tokio::spawn(run_reports(receiver, RecordedReport(Some(record))));
        sender
            .send(ReportCommand::Report(
                0,
                Session::new(Default::default(), None),
                Box::new(
                    stats
                        .pending(EndReason::TrackDone, SystemTime::now())
                        .unwrap(),
                ),
            ))
            .await
            .unwrap();
        sender.send(ReportCommand::Flush(done)).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            completed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        paused.rendered(44_100);
        drop(paused);
        assert_eq!(recorded.await.unwrap().played, Duration::from_secs(1));
        completed.await.unwrap().unwrap();
        drop(sender);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn flush_returns_reporting_failures_without_panicking() {
        let (sender, receiver) = mpsc::channel(4);
        struct FailedReport;
        impl ReportSink for FailedReport {
            async fn report(&mut self, _: u64, _: Session, _: PlaybackReport) -> Result<(), Error> {
                Err(Error::unavailable("test reporting failure"))
            }
        }
        let worker = tokio::spawn(run_reports(receiver, FailedReport));
        let mut stats = loaded();
        stats.written(SAMPLES_PER_SECOND as usize, SystemTime::now());
        let pending = PendingFlush {
            sender: sender.clone(),
            final_report: Some(ReportCommand::Report(
                0,
                Session::new(Default::default(), None),
                Box::new(
                    stats
                        .pending(EndReason::EndPlay, SystemTime::now())
                        .unwrap(),
                ),
            )),
            previous_error: None,
        };
        assert!(pending.flush().await.is_err());
        drop(sender);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn flush_waits_for_the_pending_report() {
        let (sender, receiver) = mpsc::channel(4);
        let (release, released) = oneshot::channel();
        let (done, mut completed) = oneshot::channel();
        let mut stats = loaded();
        stats.written(SAMPLES_PER_SECOND as usize, SystemTime::now());
        sender
            .send(ReportCommand::Report(
                0,
                Session::new(Default::default(), None),
                Box::new(
                    stats
                        .pending(EndReason::EndPlay, SystemTime::now())
                        .unwrap(),
                ),
            ))
            .await
            .unwrap();
        sender.send(ReportCommand::Flush(done)).await.unwrap();
        struct DelayedReport(Option<oneshot::Receiver<()>>);
        impl ReportSink for DelayedReport {
            async fn report(&mut self, _: u64, _: Session, _: PlaybackReport) -> Result<(), Error> {
                self.0.take().unwrap().await.unwrap();
                Ok(())
            }
        }
        let worker = tokio::spawn(run_reports(receiver, DelayedReport(Some(released))));
        tokio::task::yield_now().await;
        assert!(matches!(
            completed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        completed.await.unwrap().unwrap();
        drop(sender);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn saturated_queue_flush_preserves_the_final_listen() {
        let (sender, mut receiver) = mpsc::channel(1);
        let make_report = || {
            let mut stats = loaded();
            stats.written(SAMPLES_PER_SECOND as usize, SystemTime::now());
            ReportCommand::Report(
                0,
                Session::new(Default::default(), None),
                Box::new(
                    stats
                        .pending(EndReason::EndPlay, SystemTime::now())
                        .unwrap(),
                ),
            )
        };
        sender.send(make_report()).await.unwrap();
        let pending = PendingFlush {
            sender,
            final_report: Some(make_report()),
            previous_error: None,
        };
        let flush = pending.flush();
        tokio::pin!(flush);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut flush)
                .await
                .is_err()
        );
        assert!(matches!(
            receiver.recv().await,
            Some(ReportCommand::Report(..))
        ));
        let drain = async {
            assert!(matches!(
                receiver.recv().await,
                Some(ReportCommand::Report(..))
            ));
            let Some(ReportCommand::Flush(done)) = receiver.recv().await else {
                panic!("missing flush barrier")
            };
            done.send(Ok(())).unwrap();
        };
        let (result, ()) = tokio::join!(flush, drain);
        result.unwrap();
    }

    #[test]
    fn session_replacement_separates_delivered_audio() {
        let mut original = loaded();
        let now = SystemTime::now();
        original.written(SAMPLES_PER_SECOND as usize, now);
        let mut replacement = original.restart();
        assert_eq!(
            original.finish(EndReason::EndPlay, now).unwrap().played,
            Duration::from_secs(1)
        );
        assert!(
            replacement
                .restart()
                .finish(EndReason::EndPlay, now)
                .is_none()
        );
        replacement.written(SAMPLES_PER_SECOND as usize * 2, now);
        assert_eq!(
            replacement.finish(EndReason::EndPlay, now).unwrap().played,
            Duration::from_secs(2)
        );
    }

    #[test]
    fn loading_without_delivering_audio_does_not_report_a_listen() {
        assert!(
            loaded()
                .finish(EndReason::EndPlay, SystemTime::now())
                .is_none()
        );
    }

    #[test]
    fn pause_and_seek_time_do_not_inflate_played_audio() {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let mut stats = loaded();
        // Each packet is shorter than a millisecond. Preserve the remainder
        // instead of truncating the duration of every packet independently.
        for _ in 0..1000 {
            stats.written(SAMPLES_PER_SECOND as usize / 1000, start);
        }
        // Pauses and seeks move wall time / position without writing samples.
        stats.written(
            SAMPLES_PER_SECOND as usize * 2,
            start + Duration::from_secs(60),
        );
        let report = stats
            .finish(EndReason::TrackDone, start + Duration::from_secs(61))
            .unwrap();
        assert_eq!(report.played.as_millis(), 2997);
        assert_eq!(report.started_at, start);
        assert_eq!(report.audio_format, "Vorbis 320 kbps");
    }
}
